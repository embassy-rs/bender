package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/go-github/v88/github"
	"github.com/sqlbunny/errors"
)

const (
	sessionCookie = "bender_session"
	stateCookie   = "bender_oauth_state"

	// Sessions never expire. They're stateless signed cookies, so the only way to
	// invalidate one is to rotate github.session_secret, which logs everyone out
	// at once. This is just how long the browser is asked to keep the cookie.
	sessionCookieMaxAge = 10 * 365 * 24 * time.Hour

	// How long the user has to complete the redirect to GitHub and back.
	oauthStateTTL = 10 * time.Minute

	// How far ahead of expiry we proactively refresh the access token.
	tokenRefreshMargin = 2 * time.Minute
)

// errSessionExpired means the token in the session cookie is no longer accepted
// by GitHub, so the user has to log in again.
var errSessionExpired = errors.New("session expired")

// session is everything we remember about a logged-in user. It lives entirely
// in a signed cookie, so the server stores nothing.
type session struct {
	User  string `json:"u"`
	Token string `json:"t"` // GitHub user token, used to check repo permissions.

	// Set only when the GitHub App has expiring user tokens turned on. With it
	// off, GitHub hands out a token that never expires and these stay empty.
	Refresh   string `json:"r,omitempty"`
	ExpiresAt int64  `json:"e,omitempty"` // unix seconds; zero means "never".
}

// needsRefresh reports whether the access token is close enough to expiry that
// we should trade the refresh token in before using it. The margin covers both
// clock skew and the time the request itself takes.
func (sess *session) needsRefresh() bool {
	if sess.Refresh == "" || sess.ExpiresAt == 0 {
		return false
	}
	return time.Now().Add(tokenRefreshMargin).Unix() >= sess.ExpiresAt
}

// newSession builds a session from a GitHub token response.
func newSession(user string, token *oauthToken) *session {
	sess := &session{
		User:    user,
		Token:   token.AccessToken,
		Refresh: token.RefreshToken,
	}
	if token.ExpiresIn > 0 {
		sess.ExpiresAt = time.Now().Add(time.Duration(token.ExpiresIn) * time.Second).Unix()
	}
	return sess
}

// saveSession writes the session back into the signed cookie. It has to be
// called again whenever the token is refreshed, or the next request would come
// back with the stale one.
func (s *Service) saveSession(w http.ResponseWriter, sess *session) error {
	buf, err := json.Marshal(sess)
	if err != nil {
		return err
	}
	s.setCookie(w, sessionCookie, s.sign(buf), sessionCookieMaxAge)
	return nil
}

// authEnabled reports whether login is configured. Without it the UI stays
// read-only for everyone, which is the pre-login behavior.
func (s *Service) authEnabled() bool {
	g := s.config.Github
	return g.ClientID != "" && g.ClientSecret != "" && g.SessionSecret != ""
}

// sign returns payload plus an HMAC over it, so we can hand state to the
// browser and still trust it when it comes back.
func (s *Service) sign(payload []byte) string {
	mac := hmac.New(sha256.New, []byte(s.config.Github.SessionSecret))
	mac.Write(payload)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// unsign verifies a value produced by sign and returns its payload.
func (s *Service) unsign(v string) ([]byte, bool) {
	payloadB64, sigB64, ok := strings.Cut(v, ".")
	if !ok {
		return nil, false
	}
	payload, err := base64.RawURLEncoding.DecodeString(payloadB64)
	if err != nil {
		return nil, false
	}
	sig, err := base64.RawURLEncoding.DecodeString(sigB64)
	if err != nil {
		return nil, false
	}

	mac := hmac.New(sha256.New, []byte(s.config.Github.SessionSecret))
	mac.Write(payload)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, false
	}
	return payload, true
}

func (s *Service) setCookie(w http.ResponseWriter, name, value string, ttl time.Duration) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Path:     "/",
		MaxAge:   int(ttl.Seconds()),
		HttpOnly: true,
		Secure:   strings.HasPrefix(s.config.ExternalURL, "https://"),
		// Lax keeps the cookie off cross-site POSTs, which is what stops another
		// site from submitting the cancel form on a logged-in user's behalf.
		SameSite: http.SameSiteLaxMode,
	})
}

func (s *Service) clearCookie(w http.ResponseWriter, name string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   strings.HasPrefix(s.config.ExternalURL, "https://"),
		SameSite: http.SameSiteLaxMode,
	})
}

// currentUser returns the logged-in user, or nil if there's no valid session.
// A malformed or expired cookie is simply "not logged in", not an error.
func (s *Service) currentUser(r *http.Request) *session {
	if !s.authEnabled() {
		return nil
	}

	c, err := r.Cookie(sessionCookie)
	if err != nil {
		return nil
	}
	payload, ok := s.unsign(c.Value)
	if !ok {
		return nil
	}

	var sess session
	if err := json.Unmarshal(payload, &sess); err != nil {
		return nil
	}
	return &sess
}

// safeReturnPath sanitizes a post-login redirect target so it can only point
// back into this site, never at an attacker-supplied host.
func safeReturnPath(p string) string {
	if !strings.HasPrefix(p, "/") || strings.HasPrefix(p, "//") {
		return "/"
	}
	return p
}

func (s *Service) HandleLogin(w http.ResponseWriter, r *http.Request) {
	if !s.authEnabled() {
		http.Error(w, "login is not configured", http.StatusNotFound)
		return
	}

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		log.Printf("error generating oauth state: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	state := hex.EncodeToString(nonce)

	// The state is kept in a signed cookie and echoed back by GitHub, so the two
	// can be checked against each other without any server-side storage. The
	// page to return to afterwards rides along in the same cookie.
	s.setCookie(w, stateCookie, s.sign([]byte(state+"|"+safeReturnPath(r.URL.Query().Get("next")))), oauthStateTTL)

	q := url.Values{
		"client_id":    {s.config.Github.ClientID},
		"redirect_uri": {s.config.ExternalURL + "/auth/callback"},
		"state":        {state},
	}
	http.Redirect(w, r, "https://github.com/login/oauth/authorize?"+q.Encode(), http.StatusFound)
}

func (s *Service) HandleAuthCallback(w http.ResponseWriter, r *http.Request) {
	if !s.authEnabled() {
		http.Error(w, "login is not configured", http.StatusNotFound)
		return
	}

	c, err := r.Cookie(stateCookie)
	if err != nil {
		http.Error(w, "login expired, please try again", http.StatusBadRequest)
		return
	}
	s.clearCookie(w, stateCookie)

	payload, ok := s.unsign(c.Value)
	if !ok {
		http.Error(w, "invalid login state", http.StatusBadRequest)
		return
	}
	wantState, next, _ := strings.Cut(string(payload), "|")
	if subtle.ConstantTimeCompare([]byte(wantState), []byte(r.URL.Query().Get("state"))) != 1 {
		http.Error(w, "invalid login state", http.StatusBadRequest)
		return
	}

	token, err := s.exchangeOAuthCode(r.Context(), r.URL.Query().Get("code"))
	if err != nil {
		log.Printf("oauth code exchange failed: %v", err)
		http.Error(w, "login failed", http.StatusBadGateway)
		return
	}

	gh, err := github.NewClient(github.WithAuthToken(token.AccessToken))
	if err != nil {
		log.Printf("error creating github client for login: %v", err)
		http.Error(w, "login failed", http.StatusInternalServerError)
		return
	}
	user, _, err := gh.Users.Get(r.Context(), "")
	if err != nil {
		log.Printf("error fetching logged-in user: %v", err)
		http.Error(w, "login failed", http.StatusBadGateway)
		return
	}

	sess := newSession(user.GetLogin(), token)
	if err := s.saveSession(w, sess); err != nil {
		log.Printf("error encoding session: %v", err)
		http.Error(w, "login failed", http.StatusInternalServerError)
		return
	}

	log.Printf("Web login: %s", user.GetLogin())
	http.Redirect(w, r, safeReturnPath(next), http.StatusFound)
}

func (s *Service) HandleLogout(w http.ResponseWriter, r *http.Request) {
	s.clearCookie(w, sessionCookie)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// oauthToken is GitHub's reply to a token request. RefreshToken and ExpiresIn
// are only populated for apps with expiring user tokens enabled.
type oauthToken struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int64  `json:"expires_in"` // seconds
	RefreshToken string `json:"refresh_token"`

	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

// exchangeOAuthCode trades the code GitHub sent us for a user access token.
func (s *Service) exchangeOAuthCode(ctx context.Context, code string) (*oauthToken, error) {
	if code == "" {
		return nil, errors.New("no code in callback")
	}
	return s.oauthTokenRequest(ctx, url.Values{
		"grant_type":   {"authorization_code"},
		"code":         {code},
		"redirect_uri": {s.config.ExternalURL + "/auth/callback"},
	})
}

// refreshOAuthToken trades a refresh token for a fresh access token. GitHub
// rotates the refresh token too, so the reply's must replace the stored one.
func (s *Service) refreshOAuthToken(ctx context.Context, refresh string) (*oauthToken, error) {
	return s.oauthTokenRequest(ctx, url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refresh},
	})
}

// oauthTokenRequest posts to GitHub's token endpoint, adding our app
// credentials to the caller's grant-specific parameters.
func (s *Service) oauthTokenRequest(ctx context.Context, form url.Values) (*oauthToken, error) {
	form.Set("client_id", s.config.Github.ClientID)
	form.Set("client_secret", s.config.Github.ClientSecret)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://github.com/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var res oauthToken
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return nil, errors.Errorf("decoding token response (http %d): %w", resp.StatusCode, err)
	}
	if res.Error != "" {
		return nil, errors.Errorf("github said: %s (%s)", res.Error, res.ErrorDescription)
	}
	if res.AccessToken == "" {
		return nil, errors.Errorf("github returned no access token (http %d)", resp.StatusCode)
	}
	return &res, nil
}

// refreshIfNeeded renews the access token when it's expired or nearly so, and
// writes the result back to the cookie. It returns errSessionExpired when the
// refresh token itself is no longer good (they expire after six months, and a
// user revoking the app kills them immediately), which means the only way
// forward is another trip through login.
//
// Sessions with no refresh token — the app's user tokens don't expire — are
// left alone.
func (s *Service) refreshIfNeeded(ctx context.Context, w http.ResponseWriter, sess *session) error {
	if !sess.needsRefresh() {
		return nil
	}

	token, err := s.refreshOAuthToken(ctx, sess.Refresh)
	if err != nil {
		log.Printf("refreshing token for %s failed, forcing re-login: %v", sess.User, err)
		return errSessionExpired
	}

	*sess = *newSession(sess.User, token)
	return s.saveSession(w, sess)
}

// canPushTo reports whether the logged-in user has push access to a repo.
//
// This deliberately asks GitHub using the user's own token rather than Bender's
// app installation: the installation could only answer "can the app see this
// repo", which is not the same question as "may this person cancel this job".
func (sess *session) canPushTo(ctx context.Context, owner, name string) (bool, error) {
	gh, err := github.NewClient(github.WithAuthToken(sess.Token))
	if err != nil {
		return false, err
	}
	repo, _, err := gh.Repositories.Get(ctx, owner, name)
	if is401(err) {
		// The token in the cookie is dead: expired, or the user revoked our
		// authorization. The session itself is still validly signed, so the only
		// way out is to send them through login again.
		return false, errSessionExpired
	}
	if is404(err) {
		// With a user token, a repo the user can't see is indistinguishable from
		// one that doesn't exist. Either way they can't push to it.
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// Push implies maintain and admin, so this one check covers all of them.
	return repo.GetPermissions().GetPush(), nil
}
