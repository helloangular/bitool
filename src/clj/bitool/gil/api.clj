(ns bitool.gil.api
  (:require [bitool.gil.normalize :as normalize]
            [bitool.gil.validator :as validator]
            [bitool.gil.compiler :as compiler]
            [bitool.db :as db]
            [clojure.walk :as walk]
            [ring.util.http-response :as http-response]))

(defn- body-params [request]
  (or (:params request) {}))

(defn validate-handler
  [request]
  (let [raw-gil  (walk/keywordize-keys (body-params request))
        norm-gil (normalize/normalize raw-gil)
        gid      (get-in request [:session :gid])
        g        (when gid (try (db/getGraph gid) (catch Exception _ nil)))
        result   (validator/validate norm-gil g)]
    (http-response/ok (assoc result :normalized norm-gil))))

(defn compile-handler
  [request]
  (let [raw-gil    (walk/keywordize-keys (body-params request))
        norm-gil   (normalize/normalize raw-gil)
        gid        (get-in request [:session :gid])
        g          (when gid (try (db/getGraph gid) (catch Exception _ nil)))
        validation (validator/validate norm-gil g)]
    (if (:valid validation)
      (http-response/ok {:valid true
                         :plan (compiler/plan-gil norm-gil)
                         :warnings (:warnings validation)})
      (http-response/bad-request validation))))

(defn apply-handler
  [request]
  (let [raw-gil    (walk/keywordize-keys (body-params request))
        norm-gil   (normalize/normalize raw-gil)
        gid        (get-in request [:session :gid])
        g          (when gid (try (db/getGraph gid) (catch Exception _ nil)))
        validation (validator/validate norm-gil g)]
    (if-not (:valid validation)
      (http-response/bad-request validation)
      (let [result  (compiler/apply-gil norm-gil (:session request))
            session (:session request)]
        (-> (http-response/ok result)
            (assoc :session (assoc session
                                   :gid (:graph-id result)
                                   :ver (:version result))))))))

(defn from-nl-handler
  [request]
  (let [{:keys [gil do-apply]} (walk/keywordize-keys (body-params request))]
    (if-not (map? gil)
      (http-response/bad-request
        {:error "NL translator is not implemented yet. Submit :gil explicitly to /gil/from-nl, or use /gil/apply."})
      (let [norm-gil (normalize/normalize gil)
            gid      (get-in request [:session :gid])
            g        (when gid (try (db/getGraph gid) (catch Exception _ nil)))
            validation (validator/validate norm-gil g)]
        (if-not (:valid validation)
          (http-response/bad-request validation)
          (if do-apply
            (let [result  (compiler/apply-gil norm-gil (:session request))
                  session (:session request)]
              (-> (http-response/ok {:gil norm-gil :validation validation :result result})
                  (assoc :session (assoc session
                                         :gid (:graph-id result)
                                         :ver (:version result)))))
            (http-response/ok {:gil norm-gil
                               :validation validation
                               :plan (compiler/plan-gil norm-gil)})))))))
