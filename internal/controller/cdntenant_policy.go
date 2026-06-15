package controller

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"fmt"
	"net"
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
	trustedClientIPEnv        = "CWM_CDN_TRUSTED_CLIENT_IP_ENABLED"
	maxRequestBodySizeBytes   = int64(1024 * 1024 * 1024)
)

var headerFieldNamePattern = regexp.MustCompile(`^[!#$%&'*+.^_` + "`" + `|~0-9A-Za-z-]+$`)

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
		ttl, err := time.ParseDuration(cache.EdgeTtl)
		if err != nil {
			return effective, fmt.Errorf("cache.edgeTtl is invalid: %w", err)
		}
		if ttl < time.Second || ttl > 30*24*time.Hour {
			return effective, fmt.Errorf("cache.edgeTtl must be between 1s and 30d")
		}
		effective.EdgeTTLSeconds = int64(ttl / time.Second)
	}
	if cache.RespectOriginCacheControl != nil {
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
	case "authorization", "cookie", "set-cookie", "host", "connection", "upgrade", "keep-alive", "transfer-encoding", "te", "trailer", "proxy-authenticate", "proxy-authorization":
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
		"CAPTCHA_SECRET_PATH":                {},
		"CAPTCHA_SIGNING_KEY_PATH":           {},
		"POP_ID":                             {},
		"ENABLE_PLATFORM_LOGS":               {},
	}
}

func envNameProtected(name string) bool {
	_, ok := protectedTenantConfigEnvNames()[strings.ToUpper(name)]
	return ok
}

func trustedClientIPConfigured() bool {
	return strings.EqualFold(os.Getenv(trustedClientIPEnv), "true")
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
	if sec := tenant.Spec.Security; sec != nil {
		if sec.IPAccess != nil && (len(sec.IPAccess.AllowCidrs) > 0 || len(sec.IPAccess.BlockCidrs) > 0) && !trustedClientIPConfigured() {
			errs = append(errs, "security.ipAccess requires trusted client IP support")
		}
		if sec.IPAccess != nil {
			for _, cidr := range append(append([]string{}, sec.IPAccess.AllowCidrs...), sec.IPAccess.BlockCidrs...) {
				if _, _, err := net.ParseCIDR(cidr); err != nil {
					errs = append(errs, fmt.Sprintf("security.ipAccess CIDR %q is invalid", cidr))
				}
			}
		}
		if sec.URLs != nil {
			errs = append(errs, validateNamedPathRules("security.urls.block", sec.URLs.Block)...)
		}
		if sec.Methods != nil {
			errs = append(errs, validateMethods(sec.Methods)...)
		}
		if sec.RateLimit != nil && sec.RateLimit.Enabled {
			if (sec.RateLimit.Key == "" || sec.RateLimit.Key == "clientIp") && !trustedClientIPConfigured() {
				errs = append(errs, "security.rateLimit key clientIp requires trusted client IP support")
			}
			if sec.RateLimit.Period != "" {
				if _, err := time.ParseDuration(sec.RateLimit.Period); err != nil {
					errs = append(errs, fmt.Sprintf("security.rateLimit.period is invalid: %v", err))
				}
			}
			if sec.RateLimit.Action == "captcha" && (tenant.Spec.Captcha == nil || !tenant.Spec.Captcha.Enabled) {
				errs = append(errs, "security.rateLimit.action captcha requires captcha.enabled=true")
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
		if captcha.CookieTtl != "" {
			errs = append(errs, validateDurationBounds("captcha.cookieTtl", captcha.CookieTtl, time.Minute, 24*time.Hour)...)
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
		}
	}
	errs = append(errs, validateRedirects(tenant.Spec.Redirects)...)
	return errs
}

func validateNamedPathRules(field string, rules []cdnv1.NamedPathRule) []string {
	var errs []string
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
	allowedMethods := map[cdnv1.HTTPMethod]struct{}{
		"GET": {}, "HEAD": {}, "POST": {}, "PUT": {}, "PATCH": {}, "DELETE": {}, "OPTIONS": {},
	}
	allowSeen := map[cdnv1.HTTPMethod]struct{}{}
	for _, method := range methods.Allow {
		if _, ok := allowedMethods[method]; !ok {
			errs = append(errs, fmt.Sprintf("security.methods.allow contains unsupported method %q", method))
		}
		if _, ok := allowSeen[method]; ok {
			errs = append(errs, fmt.Sprintf("security.methods.allow contains duplicate method %q", method))
		}
		allowSeen[method] = struct{}{}
	}
	blockSeen := map[cdnv1.HTTPMethod]struct{}{}
	for _, method := range methods.Block {
		if _, ok := allowedMethods[method]; !ok {
			errs = append(errs, fmt.Sprintf("security.methods.block contains unsupported method %q", method))
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
	if strings.Contains(path, "/../") || strings.HasSuffix(path, "/..") {
		return fmt.Errorf("must not contain path traversal segments")
	}
	return nil
}

func validateDurationBounds(field, value string, min, max time.Duration) []string {
	duration, err := time.ParseDuration(value)
	if err != nil {
		return []string{fmt.Sprintf("%s is invalid: %v", field, err)}
	}
	if duration < min || duration > max {
		return []string{fmt.Sprintf("%s must be between %s and %s", field, min, max)}
	}
	return nil
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
	bytes := parsed * multiplier
	if bytes <= 0 || bytes > maxRequestBodySizeBytes {
		return 0, fmt.Errorf("must be between 1 byte and 1Gi")
	}
	return bytes, nil
}

func validateRedirects(redirects []cdnv1.RedirectRule) []string {
	var errs []string
	seen := map[string]struct{}{}
	for _, redirect := range redirects {
		if redirect.Name == "" {
			errs = append(errs, "redirect name is required")
		}
		if _, ok := seen[redirect.Name]; redirect.Name != "" && ok {
			errs = append(errs, fmt.Sprintf("redirect name %q is duplicated", redirect.Name))
		}
		seen[redirect.Name] = struct{}{}
		if redirect.When.Path.Type != "glob" {
			errs = append(errs, fmt.Sprintf("redirect %q when.path.type must be glob", redirect.Name))
		}
		if err := validateGlobPath(redirect.When.Path.Value); err != nil {
			errs = append(errs, fmt.Sprintf("redirect %q when.path.value is invalid: %v", redirect.Name, err))
		}
		if len(redirect.To) > 512 || strings.ContainsAny(redirect.To, "\r\n") || !(strings.HasPrefix(redirect.To, "/") || strings.HasPrefix(redirect.To, "http://") || strings.HasPrefix(redirect.To, "https://")) {
			errs = append(errs, fmt.Sprintf("redirect %q to must be a relative path or http/https URL without CR/LF", redirect.Name))
		}
		if len(redirect.When.OriginStatus) > 0 && len(redirect.When.UpstreamStatus) > 0 {
			errs = append(errs, fmt.Sprintf("redirect %q originStatus and upstreamStatus are mutually exclusive", redirect.Name))
		}
		errs = append(errs, validateHTTPStatuses(fmt.Sprintf("redirect %q originStatus", redirect.Name), redirect.When.OriginStatus)...)
		errs = append(errs, validateHTTPStatuses(fmt.Sprintf("redirect %q upstreamStatus", redirect.Name), redirect.When.UpstreamStatus)...)
		if redirect.Status != 0 && redirect.Status != 301 && redirect.Status != 302 && redirect.Status != 307 && redirect.Status != 308 {
			errs = append(errs, fmt.Sprintf("redirect %q status must be one of 301, 302, 307, 308", redirect.Name))
		}
	}
	return errs
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
