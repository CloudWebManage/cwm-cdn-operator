package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"fmt"
	"net"
	neturl "net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	cdnv1 "github.com/CloudWebManage/cwm-cdn-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	policyConfigMapName       = "tenant-policy"
	policyConfigMapKey        = "policy.json"
	policyMountPath           = "/etc/cwm-cdn"
	policyFilePath            = policyMountPath + "/" + policyConfigMapKey
	captchaSecretMountPath    = "/etc/cwm-cdn/captcha-secret"
	signingKeySecretName      = "tenant-policy-signing-key"
	signingKeySecretKey       = "signing-key"
	signingKeySecretMountPath = "/etc/cwm-cdn/signing-key"
	maxRequestBodySizeBytes   = int64(1024 * 1024 * 1024)
	maxPolicyRules            = 100
	maxPolicyMethods          = 32
)

var (
	headerFieldNamePattern = regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)
	policyDurationPattern  = regexp.MustCompile(`^(\d+)(s|m|h|d)$`)
	methodPattern          = regexp.MustCompile(`^[A-Z]+$`)
	globPathPattern        = regexp.MustCompile(`^/[A-Za-z0-9._~/%:@+*,=-]*$`)
	redirectTargetPattern  = regexp.MustCompile(`^(?:/[A-Za-z0-9._~!&'()*+,=:@/%?-]*|https?://[A-Za-z0-9._~!&'()*+,=:@/%?-]+)$`)
)

type effectiveCacheConfig struct {
	Enabled                   bool   `json:"enabled"`
	Mode                      string `json:"mode"`
	EdgeTTLSeconds            int64  `json:"edgeTtlSeconds"`
	RespectOriginCacheControl bool   `json:"respectOriginCacheControl"`
	StatusHeader              string `json:"statusHeader"`
}

func effectiveCache(cache *cdnv1.CacheConfig) (effectiveCacheConfig, error) {
	effective := effectiveCacheConfig{
		Enabled:                   true,
		Mode:                      "cache_everything",
		EdgeTTLSeconds:            int64(time.Hour / time.Second),
		RespectOriginCacheControl: true,
		StatusHeader:              "X-CWM-Cache-Status",
	}
	if cache == nil {
		return effective, nil
	}
	if cache.Enabled != nil {
		effective.Enabled = *cache.Enabled
	}
	if cache.Mode != "" {
		effective.Mode = cache.Mode
	}
	if effective.Mode != "cache_everything" {
		return effective, fmt.Errorf("cache.mode must be cache_everything")
	}
	if cache.EdgeTtl != "" {
		ttlSeconds, err := parsePolicyDurationSeconds(cache.EdgeTtl, 1, 30*24*60*60)
		if err != nil {
			return effective, fmt.Errorf("cache.edgeTtl is invalid: %w", err)
		}
		if ttlSeconds != int64(time.Hour/time.Second) {
			return effective, fmt.Errorf("cache.edgeTtl values other than 1h require cache-layer dynamic TTL support")
		}
		effective.EdgeTTLSeconds = ttlSeconds
	}
	if cache.RespectOriginCacheControl != nil {
		if !*cache.RespectOriginCacheControl {
			return effective, fmt.Errorf("cache.respectOriginCacheControl=false requires cache-layer origin-header override support")
		}
		effective.RespectOriginCacheControl = *cache.RespectOriginCacheControl
	}
	if cache.StatusHeader != nil {
		effective.StatusHeader = *cache.StatusHeader
	}
	if effective.StatusHeader != "" && !validCacheStatusHeader(effective.StatusHeader) {
		return effective, fmt.Errorf("cache.statusHeader %q is not an allowed HTTP field name", effective.StatusHeader)
	}
	return effective, nil
}

func validCacheStatusHeader(name string) bool {
	if !headerFieldNamePattern.MatchString(name) {
		return false
	}
	switch strings.ToLower(name) {
	case "authorization", "cookie", "set-cookie", "host", "connection", "upgrade", "keep-alive", "transfer-encoding", "te", "trailer", "proxy-authenticate", "proxy-authorization", "x-cwmcdn-tenant-name", "x-cwmcdn-cache-enabled", "x-cwmcdn-cache-edge-ttl-seconds", "x-cwmcdn-cache-respect-origin-cache-control", "x-cwmcdn-cache-status-header":
		return false
	default:
		return true
	}
}

func appendCacheEnv(env []corev1.EnvVar, cache effectiveCacheConfig) []corev1.EnvVar {
	return append(env,
		corev1.EnvVar{Name: "CACHE_ENABLED", Value: strconv.FormatBool(cache.Enabled)},
		corev1.EnvVar{Name: "CACHE_MODE", Value: cache.Mode},
		corev1.EnvVar{Name: "CACHE_EDGE_TTL_SECONDS", Value: strconv.FormatInt(cache.EdgeTTLSeconds, 10)},
		corev1.EnvVar{Name: "CACHE_RESPECT_ORIGIN_CACHE_CONTROL", Value: strconv.FormatBool(cache.RespectOriginCacheControl)},
		corev1.EnvVar{Name: "CACHE_STATUS_HEADER", Value: cache.StatusHeader},
	)
}

func protectedTenantConfigEnvNames() map[string]struct{} {
	return map[string]struct{}{
		"IMAGE":                              {},
		"REPLICAS":                           {},
		"CACHE_ENABLED":                      {},
		"CACHE_MODE":                         {},
		"CACHE_EDGE_TTL_SECONDS":             {},
		"CACHE_RESPECT_ORIGIN_CACHE_CONTROL": {},
		"CACHE_STATUS_HEADER":                {},
		"CDN_POLICY_FILE":                    {},
		"CWM_CDN_POLICY_PATH":                {},
		"TENANT_POLICY_JSON":                 {},
		"CAPTCHA_SECRET_PATH":                {},
		"CAPTCHA_SIGNING_KEY_PATH":           {},
		"POP_ID":                             {},
		"CWM_CDN_POP_ID":                     {},
		"ENABLE_PLATFORM_LOGS":               {},
		"ENABLE_TENANT_ACCESS_LOGS":          {},
		"ENABLE_ES_SINK":                     {},
		"CDN_CACHE_ROUTER":                   {},
	}
}

func envNameProtected(name string) bool {
	_, ok := protectedTenantConfigEnvNames()[strings.ToUpper(name)]
	return ok
}

func trustedClientIPEnabled() bool {
	value := strings.ToLower(os.Getenv("CWM_CDN_TRUSTED_CLIENT_IP_ENABLED"))
	return value == "1" || value == "true" || value == "yes"
}

func validateTenantConfigMaps(tenant *cdnv1.CdnTenant) []string {
	var errs []string
	legacyAllowedConfig := map[string]struct{}{
		"IMAGE":    {},
		"REPLICAS": {},
	}
	for key := range tenant.Spec.Config {
		upperKey := strings.ToUpper(key)
		_, legacyAllowed := legacyAllowedConfig[upperKey]
		if envNameProtected(key) && !legacyAllowed {
			errs = append(errs, fmt.Sprintf("spec.config.%s is platform-managed and must not be set by tenants", key))
		}
	}
	domainReserved := map[string]struct{}{"NAME": {}, "CERT": {}, "KEY": {}, "TLS_MODE": {}, "TLS_MIN_VERSION": {}, "TLS_MAX_VERSION": {}, "REDIRECT_HTTP_TO_HTTPS": {}, "CERT_PATH": {}, "KEY_PATH": {}}
	for i, domain := range tenant.Spec.Domains {
		for key := range domain.Config {
			if _, ok := domainReserved[strings.ToUpper(key)]; ok {
				errs = append(errs, fmt.Sprintf("domains[%d].config.%s conflicts with platform-managed domain config", i, key))
			}
		}
	}
	originReserved := map[string]struct{}{"URL": {}, "NAME": {}, "WEIGHT": {}, "HEALTHCHECK_ENABLED": {}, "HEALTHCHECK_PATH": {}, "HEALTHCHECK_EXPECTEDSTATUS": {}, "HEALTHCHECK_INTERVAL": {}, "HEALTHCHECK_TIMEOUT": {}, "HEALTHCHECK_HEALTHYTHRESHOLD": {}, "HEALTHCHECK_UNHEALTHYTHRESHOLD": {}}
	for i, origin := range tenant.Spec.Origins {
		for key := range origin.Config {
			if _, ok := originReserved[strings.ToUpper(key)]; ok {
				errs = append(errs, fmt.Sprintf("origins[%d].config.%s conflicts with platform-managed origin config", i, key))
			}
		}
	}
	return errs
}

func captchaRequired(tenant *cdnv1.CdnTenant) bool {
	if tenant.Spec.Captcha != nil && tenant.Spec.Captcha.Enabled {
		return true
	}
	return tenant.Spec.Security != nil && tenant.Spec.Security.RateLimit != nil && tenant.Spec.Security.RateLimit.Enabled && tenant.Spec.Security.RateLimit.Action == "captcha"
}

func validateTenantPolicy(ctx context.Context, c client.Client, tenant *cdnv1.CdnTenant) []string {
	var errs []string
	if _, err := effectiveCache(tenant.Spec.Cache); err != nil {
		errs = append(errs, err.Error())
	}
	errs = append(errs, validateTenantConfigMaps(tenant)...)
	if sec := tenant.Spec.Security; sec != nil {
		if sec.IPAccess != nil {
			if (len(sec.IPAccess.AllowCidrs) > 0 || len(sec.IPAccess.BlockCidrs) > 0) && !trustedClientIPEnabled() {
				errs = append(errs, "security.ipAccess requires trusted client IP support to be enabled")
			}
			for _, cidr := range append(append([]string{}, sec.IPAccess.AllowCidrs...), sec.IPAccess.BlockCidrs...) {
				if _, _, err := net.ParseCIDR(cidr); err != nil {
					errs = append(errs, fmt.Sprintf("security.ipAccess CIDR %q is invalid", cidr))
				}
			}
			if len(sec.IPAccess.AllowCidrs) > maxPolicyRules {
				errs = append(errs, fmt.Sprintf("security.ipAccess.allowCidrs may contain at most %d CIDRs", maxPolicyRules))
			}
			if len(sec.IPAccess.BlockCidrs) > maxPolicyRules {
				errs = append(errs, fmt.Sprintf("security.ipAccess.blockCidrs may contain at most %d CIDRs", maxPolicyRules))
			}
		}
		if sec.URLs != nil {
			errs = append(errs, validateNamedPathRules("security.urls.block", sec.URLs.Block)...)
		}
		if sec.Methods != nil {
			errs = append(errs, validateMethods(sec.Methods)...)
		}
		if sec.RateLimit != nil && sec.RateLimit.Enabled {
			if sec.RateLimit.Requests < 1 {
				errs = append(errs, "security.rateLimit.requests must be a positive integer")
			}
			if sec.RateLimit.Key != "" && sec.RateLimit.Key != "clientIp" {
				errs = append(errs, "security.rateLimit.key must be clientIp")
			}
			if !trustedClientIPEnabled() {
				errs = append(errs, "security.rateLimit.key=clientIp requires trusted client IP support to be enabled")
			}
			if sec.RateLimit.Period != "" {
				if _, err := parsePolicyDurationSeconds(sec.RateLimit.Period, 1, 3600); err != nil {
					errs = append(errs, fmt.Sprintf("security.rateLimit.period is invalid: %v", err))
				}
			}
			if sec.RateLimit.Action == "captcha" {
				if tenant.Spec.Captcha == nil || !tenant.Spec.Captcha.Enabled {
					errs = append(errs, "security.rateLimit.action=captcha requires captcha.enabled=true")
				}
			}
			if sec.RateLimit.Action != "" && sec.RateLimit.Action != "block" && sec.RateLimit.Action != "captcha" {
				errs = append(errs, "security.rateLimit.action must be block or captcha")
			}
		}
		if sec.Request != nil && sec.Request.MaxBodySize != "" {
			if _, err := parseMaxBodySize(sec.Request.MaxBodySize); err != nil {
				errs = append(errs, fmt.Sprintf("security.request.maxBodySize is invalid: %v", err))
			}
		}
	}
	if captcha := tenant.Spec.Captcha; captcha != nil {
		errs = append(errs, validateNamedPathRules("captcha.rules", captcha.Rules)...)
		if len(captcha.Rules) > 0 && !captcha.Enabled {
			errs = append(errs, "captcha.rules require captcha.enabled=true")
		}
		if captcha.Enabled {
			if captcha.Provider != "turnstile" {
				errs = append(errs, "captcha.provider must be turnstile when captcha is enabled")
			}
			if captcha.SiteKey == "" {
				errs = append(errs, "captcha.siteKey is required when captcha is enabled")
			}
			if captcha.SecretRef == nil || captcha.SecretRef.Name == "" || captcha.SecretRef.Key == "" {
				errs = append(errs, "captcha.secretRef.name and captcha.secretRef.key are required when captcha is enabled")
			} else if err := secretKeyExists(ctx, c, tenant.Name, captcha.SecretRef.Name, captcha.SecretRef.Key); err != nil {
				errs = append(errs, fmt.Sprintf("captcha.secretRef is not ready: %v", err))
			}
			if captcha.CookieTtl != "" {
				if _, err := parsePolicyDurationSeconds(captcha.CookieTtl, 1, 24*60*60); err != nil {
					errs = append(errs, fmt.Sprintf("captcha.cookieTtl is invalid: %v", err))
				}
			}
		}
	}
	errs = append(errs, validateRedirects(tenant.Spec.Redirects)...)
	return errs
}

func validateNamedPathRules(field string, rules []cdnv1.NamedPathRule) []string {
	var errs []string
	if len(rules) > maxPolicyRules {
		errs = append(errs, fmt.Sprintf("%s may contain at most %d rules", field, maxPolicyRules))
	}
	seen := map[string]struct{}{}
	for _, rule := range rules {
		if rule.Name == "" {
			errs = append(errs, field+" rule name is required")
		}
		if _, ok := seen[rule.Name]; rule.Name != "" && ok {
			errs = append(errs, fmt.Sprintf("%s rule name %q is duplicated", field, rule.Name))
		}
		seen[rule.Name] = struct{}{}
		if rule.Match.Type != "glob" {
			errs = append(errs, fmt.Sprintf("%s rule %q match.type must be glob", field, rule.Name))
		}
		if err := validateGlobPath(rule.Match.Path); err != nil {
			errs = append(errs, fmt.Sprintf("%s rule %q match.path is invalid: %v", field, rule.Name, err))
		}
	}
	return errs
}

func validateMethods(methods *cdnv1.MethodsConfig) []string {
	var errs []string
	if len(methods.Allow) > maxPolicyMethods {
		errs = append(errs, fmt.Sprintf("security.methods.allow may contain at most %d methods", maxPolicyMethods))
	}
	if len(methods.Block) > maxPolicyMethods {
		errs = append(errs, fmt.Sprintf("security.methods.block may contain at most %d methods", maxPolicyMethods))
	}
	allowSeen := map[cdnv1.HTTPMethod]struct{}{}
	for _, method := range methods.Allow {
		if !methodPattern.MatchString(string(method)) {
			errs = append(errs, fmt.Sprintf("security.methods.allow contains invalid HTTP method %q", method))
		}
		if _, ok := allowSeen[method]; ok {
			errs = append(errs, fmt.Sprintf("security.methods.allow contains duplicate method %q", method))
		}
		allowSeen[method] = struct{}{}
	}
	blockSeen := map[cdnv1.HTTPMethod]struct{}{}
	for _, method := range methods.Block {
		if !methodPattern.MatchString(string(method)) {
			errs = append(errs, fmt.Sprintf("security.methods.block contains invalid HTTP method %q", method))
		}
		if _, ok := blockSeen[method]; ok {
			errs = append(errs, fmt.Sprintf("security.methods.block contains duplicate method %q", method))
		}
		if _, ok := allowSeen[method]; ok {
			errs = append(errs, fmt.Sprintf("security.methods method %q cannot be both allowed and blocked", method))
		}
		blockSeen[method] = struct{}{}
	}
	return errs
}

func validateGlobPath(path string) error {
	if path == "" || len(path) > 512 {
		return fmt.Errorf("must be 1-512 characters")
	}
	if !strings.HasPrefix(path, "/") {
		return fmt.Errorf("must start with /")
	}
	if strings.ContainsAny(path, "\r\n\x00") {
		return fmt.Errorf("must not contain control characters")
	}
	if strings.Contains(path, "\\") {
		return fmt.Errorf("must not contain backslashes")
	}
	if !globPathPattern.MatchString(path) {
		return fmt.Errorf("contains unsafe characters")
	}
	if strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") || strings.Contains(path, "/./") || strings.HasSuffix(path, "/.") {
		return fmt.Errorf("must not contain path traversal segments")
	}
	lowerPath := strings.ToLower(path)
	if strings.Contains(lowerPath, "%2e") || strings.Contains(lowerPath, "%2f") || strings.Contains(lowerPath, "%5c") {
		return fmt.Errorf("must not contain encoded path traversal characters")
	}
	return nil
}

func parsePolicyDurationSeconds(value string, minSeconds, maxSeconds int64) (int64, error) {
	match := policyDurationPattern.FindStringSubmatch(value)
	if match == nil {
		return 0, fmt.Errorf("must use s, m, h, or d duration syntax")
	}
	amount, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil || amount <= 0 {
		return 0, fmt.Errorf("must be a positive duration")
	}
	multiplier := int64(1)
	switch match[2] {
	case "s":
		multiplier = 1
	case "m":
		multiplier = 60
	case "h":
		multiplier = 3600
	case "d":
		multiplier = 86400
	}
	if amount > maxSeconds/multiplier {
		return 0, fmt.Errorf("must be between %ds and %ds", minSeconds, maxSeconds)
	}
	seconds := amount * multiplier
	if seconds < minSeconds || seconds > maxSeconds {
		return 0, fmt.Errorf("must be between %ds and %ds", minSeconds, maxSeconds)
	}
	return seconds, nil
}

func parseMaxBodySize(value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	multiplier := int64(1)
	numeric := value
	switch suffix := value[len(value)-1:]; suffix {
	case "k", "K":
		multiplier = 1024
		numeric = value[:len(value)-1]
	case "m", "M":
		multiplier = 1024 * 1024
		numeric = value[:len(value)-1]
	case "g", "G":
		multiplier = 1024 * 1024 * 1024
		numeric = value[:len(value)-1]
	}
	parsed, err := strconv.ParseInt(numeric, 10, 64)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("must be a positive integer optionally suffixed with k, m, or g")
	}
	if parsed > maxRequestBodySizeBytes/multiplier {
		return 0, fmt.Errorf("must be between 1 byte and 1Gi")
	}
	bytes := parsed * multiplier
	if bytes <= 0 || bytes > maxRequestBodySizeBytes {
		return 0, fmt.Errorf("must be between 1 byte and 1Gi")
	}
	return bytes, nil
}

func validateRedirects(redirects []cdnv1.RedirectRule) []string {
	var errs []string
	if len(redirects) > maxPolicyRules {
		errs = append(errs, fmt.Sprintf("redirects may contain at most %d rules", maxPolicyRules))
	}
	seen := map[string]struct{}{}
	for _, redirect := range redirects {
		if redirect.Name == "" {
			errs = append(errs, "redirect name is required")
		}
		if _, ok := seen[redirect.Name]; redirect.Name != "" && ok {
			errs = append(errs, fmt.Sprintf("redirect name %q is duplicated", redirect.Name))
		}
		seen[redirect.Name] = struct{}{}
		hasPath := redirect.When.Path != nil && (redirect.When.Path.Type != "" || redirect.When.Path.Value != "")
		hasOriginStatus := len(redirect.When.OriginStatus) > 0
		hasUpstreamStatus := len(redirect.When.UpstreamStatus) > 0
		if !hasPath && !hasOriginStatus && !hasUpstreamStatus {
			errs = append(errs, fmt.Sprintf("redirect %q requires at least one matcher", redirect.Name))
		}
		if hasOriginStatus && hasUpstreamStatus {
			errs = append(errs, fmt.Sprintf("redirect %q originStatus and upstreamStatus are mutually exclusive", redirect.Name))
		}
		if hasPath {
			if redirect.When.Path.Type != "glob" {
				errs = append(errs, fmt.Sprintf("redirect %q when.path.type must be glob", redirect.Name))
			}
			if err := validateGlobPath(redirect.When.Path.Value); err != nil {
				errs = append(errs, fmt.Sprintf("redirect %q when.path.value is invalid: %v", redirect.Name, err))
			}
		}
		if err := validateRedirectTarget(redirect.To); err != nil {
			errs = append(errs, fmt.Sprintf("redirect %q to is invalid: %v", redirect.Name, err))
		}
		errs = append(errs, validateHTTPStatuses(fmt.Sprintf("redirect %q originStatus", redirect.Name), redirect.When.OriginStatus)...)
		errs = append(errs, validateHTTPStatuses(fmt.Sprintf("redirect %q upstreamStatus", redirect.Name), redirect.When.UpstreamStatus)...)
		if hasPath && !strings.Contains(redirect.When.Path.Value, "*") && strings.SplitN(redirect.To, "?", 2)[0] == redirect.When.Path.Value {
			errs = append(errs, fmt.Sprintf("redirect %q direct self-redirects are not allowed", redirect.Name))
		}
		if redirect.Status != 0 && redirect.Status != 301 && redirect.Status != 302 && redirect.Status != 307 && redirect.Status != 308 {
			errs = append(errs, fmt.Sprintf("redirect %q status must be one of 301, 302, 307, 308", redirect.Name))
		}
	}
	return errs
}

func validateRedirectTarget(target string) error {
	if target == "" || len(target) > 512 {
		return fmt.Errorf("must be 1-512 characters")
	}
	if strings.ContainsAny(target, "\r\n\x00") {
		return fmt.Errorf("must not contain control characters")
	}
	if strings.Contains(target, "\\") {
		return fmt.Errorf("must not contain backslashes")
	}
	if !redirectTargetPattern.MatchString(target) || strings.ContainsAny(target, " $;{}") {
		return fmt.Errorf("must be a safe relative /... or absolute http(s) URL")
	}
	if strings.HasPrefix(target, "/") {
		if strings.HasPrefix(target, "//") {
			return fmt.Errorf("relative paths must not start with //")
		}
		return nil
	}
	parsed, err := neturl.Parse(target)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("must be a relative path or absolute http/https URL")
	}
	if parsed.Host == "" {
		return fmt.Errorf("absolute URLs must include a host")
	}
	if parsed.User != nil {
		return fmt.Errorf("absolute URLs must not include user info")
	}
	return nil
}

func validateHTTPStatuses(field string, statuses []cdnv1.HTTPStatusCode) []string {
	var errs []string
	seen := map[cdnv1.HTTPStatusCode]struct{}{}
	for _, status := range statuses {
		if status < 100 || status > 599 {
			errs = append(errs, fmt.Sprintf("%s contains invalid HTTP status %d", field, status))
		}
		if _, ok := seen[status]; ok {
			errs = append(errs, fmt.Sprintf("%s contains duplicate HTTP status %d", field, status))
		}
		seen[status] = struct{}{}
	}
	return errs
}

func secretKeyExists(ctx context.Context, c client.Client, namespace, name, key string) error {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		return err
	}
	if _, ok := secret.Data[key]; !ok {
		return fmt.Errorf("key %q not found in Secret %s/%s", key, namespace, name)
	}
	return nil
}

func secretRolloutHash(ctx context.Context, c client.Client, namespace, name, key string) (string, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, secret); err != nil {
		return "", err
	}
	if key != "" {
		if _, ok := secret.Data[key]; !ok {
			return "", fmt.Errorf("key %q not found in Secret %s/%s", key, namespace, name)
		}
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%s", namespace, name, key, secret.ResourceVersion)))
	return hex.EncodeToString(h[:]), nil
}

func buildPolicyJSON(tenant *cdnv1.CdnTenant, cache effectiveCacheConfig) (string, string, error) {
	policy := map[string]interface{}{
		"cache":     cache,
		"security":  tenant.Spec.Security,
		"captcha":   tenant.Spec.Captcha,
		"redirects": tenant.Spec.Redirects,
	}
	b, err := stdjson.MarshalIndent(policy, "", "  ")
	if err != nil {
		return "", "", err
	}
	h := sha256.Sum256(b)
	return string(b), hex.EncodeToString(h[:]), nil
}

func ensureSigningKeySecret(ctx context.Context, c client.Client, tenant *cdnv1.CdnTenant) error {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: signingKeySecretName, Namespace: tenant.Name}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}
		data, err := randomSecretData(32)
		if err != nil {
			return err
		}
		secret = &corev1.Secret{
			ObjectMeta: tenantOwnedObjectMeta(signingKeySecretName, tenant),
			Type:       corev1.SecretTypeOpaque,
			Data:       map[string][]byte{signingKeySecretKey: data},
		}
		return c.Create(ctx, secret)
	}
	if _, ok := secret.Data[signingKeySecretKey]; ok {
		return nil
	}
	data, err := randomSecretData(32)
	if err != nil {
		return err
	}
	if secret.Data == nil {
		secret.Data = map[string][]byte{}
	}
	secret.Data[signingKeySecretKey] = data
	return c.Update(ctx, secret)
}

func randomSecretData(size int) ([]byte, error) {
	data := make([]byte, size)
	_, err := rand.Read(data)
	return data, err
}

func tenantOwnedObjectMeta(name string, tenant *cdnv1.CdnTenant) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: tenant.Name,
		Annotations: map[string]string{
			tenantAnnotationKey: fmt.Sprintf("%s/%s", tenant.Namespace, tenant.Name),
		},
		Labels: map[string]string{
			"cdn.cloudwm-cdn.com/tenant": tenant.Name,
		},
	}
}

func redactedSecondaryURL(raw string) string {
	if raw == "" {
		return ""
	}
	withoutCreds := raw
	if strings.Contains(withoutCreds, "@") {
		parts := strings.SplitN(withoutCreds, "@", 2)
		withoutCreds = parts[1]
	}
	return withoutCreds
}

func stableSecondaryNames(secondaries map[string]map[string]string) []string {
	names := make([]string, 0, len(secondaries))
	for name := range secondaries {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
