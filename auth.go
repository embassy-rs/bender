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
)

// session is everything we remember about a logged-in user. It lives entirely
// in a signed cookie, so the server stores nothing.
type session struct {
	User  string `json:"u"`
	Token string `json:"t"` // GitHub user token, used to check repo permissions.
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

	gh, err := github.NewClient(github.WithAuthToken(token))
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

	buf, err := json.Marshal(session{
		User:  user.GetLogin(),
		Token: token,
	})
	if err != nil {
		log.Printf("error encoding session: %v", err)
		http.Error(w, "login failed", http.StatusInternalServerError)
		return
	}
	s.setCookie(w, sessionCookie, s.sign(buf), sessionCookieMaxAge)

	log.Printf("Web login: %s", user.GetLogin())
	http.Redirect(w, r, safeReturnPath(next), http.StatusFound)
}

func (s *Service) HandleLogout(w http.ResponseWriter, r *http.Request) {
	s.clearCookie(w, sessionCookie)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// exchangeOAuthCode trades the code GitHub sent us for a user access token.
func (s *Service) exchangeOAuthCode(ctx context.Context, code string) (string, error) {
	if code == "" {
		return "", errors.New("no code in callback")
	}

	form := url.Values{
		"client_id":     {s.config.Github.ClientID},
		"client_secret": {s.config.Github.ClientSecret},
		"code":          {code},
		"redirect_uri":  {s.config.ExternalURL + "/auth/callback"},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"https://github.com/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var res struct {
		AccessToken      string `json:"access_token"`
		Error            string `json:"error"`
		ErrorDescription string `json:"error_description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", errors.Errorf("decoding token response (http %d): %w", resp.StatusCode, err)
	}
	if res.Error != "" {
		return "", errors.Errorf("github said: %s (%s)", res.Error, res.ErrorDescription)
	}
	if res.AccessToken == "" {
		return "", errors.Errorf("github returned no access token (http %d)", resp.StatusCode)
	}
	return res.AccessToken, nil
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
	if err != nil {
		return false, err
	}
	// Push implies maintain and admin, so this one check covers all of them.
	return repo.GetPermissions().GetPush(), nil
}
