(ns bitool.connector.auth)

(defmulti build-auth
  "Returns headers or query params for the given auth method."
  (fn [auth _] (:type auth)))

(defmethod build-auth :none [_ _] {:headers {} :query {}})

(defmethod build-auth :basic [{:keys [username password]} _]
  (let [bytes (.getBytes (str username ":" password) "UTF-8")
        encoder (java.util.Base64/getEncoder)
        creds (.encodeToString encoder bytes)]
    {:headers {"Authorization" (str "Basic " creds)}}))

(defmethod build-auth :bearer [{:keys [token]} _]
  {:headers {"Authorization" (str "Bearer " token)}})

(defmethod build-auth :api-key [{:keys [key location param-name header-name]} _]
  (case location
    :header {:headers {header-name key}}
    :query  {:query {param-name key}}))

(defmethod build-auth :custom-header [{:keys [headers]} _]
  {:headers headers})

(defmethod build-auth :oauth2 [{:keys [access-token]} _]
  {:headers {"Authorization" (str "Bearer " access-token)}})

;; NEW: Atlassian Cloud convenience (email + API token)
(defmethod build-auth :atlassian-basic [{:keys [email api-token]} _]
  (let [bytes (.getBytes (str email ":" api-token) "UTF-8")
        encoder (java.util.Base64/getEncoder)
        creds (.encodeToString encoder bytes)]
    {:headers {"Authorization" (str "Basic " creds)}}))
