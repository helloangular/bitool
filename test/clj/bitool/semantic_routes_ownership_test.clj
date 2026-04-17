(ns bitool.semantic-routes-ownership-test
  "Tests that nested resource routes enforce model_id ownership.
   A caller with a valid model route must NOT be able to read or delete
   resources (perspectives, RLS policies) belonging to a different model."
  (:require [clojure.test :refer :all]
            [bitool.semantic.routes :as routes]
            [bitool.semantic.perspective :as persp]
            [bitool.semantic.governance :as gov]
            [cheshire.core :as json]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- parse-body [response]
  (when-let [body (:body response)]
    (try (json/parse-string body true) (catch Exception _ nil))))

(defn- mock-request [path-params]
  {:path-params path-params
   :params      {}
   :session     {:user "test-user" :roles ["admin"]}})

;; ---------------------------------------------------------------------------
;; Perspective ownership — get-perspective
;; ---------------------------------------------------------------------------

(deftest get-perspective-returns-404-when-model-id-mismatch
  ;; Perspective 50 belongs to model 10, but caller requests via model 99
  (with-redefs [persp/get-perspective       (fn [pid] {:perspective_id 50 :model_id 10 :name "finance"})
                persp/ensure-perspective-tables! (fn [])]
    (let [response (routes/get-perspective (mock-request {:model_id "99" :perspective_id "50"}))]
      (is (= 404 (:status response)))
      (is (not (:ok (parse-body response)))))))

(deftest get-perspective-returns-200-when-model-id-matches
  (with-redefs [persp/get-perspective       (fn [pid] {:perspective_id 50 :model_id 10 :name "finance"})
                persp/ensure-perspective-tables! (fn [])]
    (let [response (routes/get-perspective (mock-request {:model_id "10" :perspective_id "50"}))]
      (is (= 200 (:status response)))
      (is (:ok (parse-body response))))))

(deftest get-perspective-returns-404-when-perspective-not-found
  (with-redefs [persp/get-perspective       (fn [pid] nil)
                persp/ensure-perspective-tables! (fn [])]
    (let [response (routes/get-perspective (mock-request {:model_id "10" :perspective_id "999"}))]
      (is (= 404 (:status response))))))

;; ---------------------------------------------------------------------------
;; Perspective ownership — delete-perspective
;; ---------------------------------------------------------------------------

(deftest delete-perspective-returns-404-when-model-id-mismatch
  (with-redefs [persp/get-perspective       (fn [pid] {:perspective_id 50 :model_id 10})
                persp/delete-perspective!    (fn [pid] (throw (ex-info "Should not be called" {})))
                persp/ensure-perspective-tables! (fn [])]
    (let [response (routes/delete-perspective (mock-request {:model_id "99" :perspective_id "50"}))]
      (is (= 404 (:status response)))
      ;; Critical: delete-perspective! must NOT have been called
      (is (not (:ok (parse-body response)))))))

(deftest delete-perspective-proceeds-when-model-id-matches
  (let [deleted (atom false)]
    (with-redefs [persp/get-perspective       (fn [pid] {:perspective_id 50 :model_id 10})
                  persp/delete-perspective!    (fn [pid] (reset! deleted true) {:perspective_id pid})
                  persp/ensure-perspective-tables! (fn [])]
      (let [response (routes/delete-perspective (mock-request {:model_id "10" :perspective_id "50"}))]
        (is (= 200 (:status response)))
        (is @deleted "delete-perspective! should have been called")))))

(deftest delete-perspective-returns-404-when-not-found
  (with-redefs [persp/get-perspective       (fn [pid] nil)
                persp/delete-perspective!    (fn [pid] (throw (ex-info "Should not be called" {})))
                persp/ensure-perspective-tables! (fn [])]
    (let [response (routes/delete-perspective (mock-request {:model_id "10" :perspective_id "999"}))]
      (is (= 404 (:status response))))))

;; ---------------------------------------------------------------------------
;; RLS policy ownership — delete-rls-policy
;; ---------------------------------------------------------------------------

(deftest delete-rls-policy-returns-404-when-model-id-mismatch
  (with-redefs [gov/get-rls-policy      (fn [pid] {:policy_id 30 :model_id 10})
                gov/delete-rls-policy!   (fn [pid] (throw (ex-info "Should not be called" {})))
                gov/ensure-governance-tables! (fn [])]
    (let [response (routes/delete-rls-policy (mock-request {:model_id "99" :policy_id "30"}))]
      (is (= 404 (:status response)))
      (is (not (:ok (parse-body response)))))))

(deftest delete-rls-policy-proceeds-when-model-id-matches
  (let [deleted (atom false)]
    (with-redefs [gov/get-rls-policy      (fn [pid] {:policy_id 30 :model_id 10})
                  gov/delete-rls-policy!   (fn [pid] (reset! deleted true) {:policy_id pid})
                  gov/ensure-governance-tables! (fn [])]
      (let [response (routes/delete-rls-policy (mock-request {:model_id "10" :policy_id "30"}))]
        (is (= 200 (:status response)))
        (is @deleted "delete-rls-policy! should have been called")))))

(deftest delete-rls-policy-returns-404-when-not-found
  (with-redefs [gov/get-rls-policy      (fn [pid] nil)
                gov/delete-rls-policy!   (fn [pid] (throw (ex-info "Should not be called" {})))
                gov/ensure-governance-tables! (fn [])]
    (let [response (routes/delete-rls-policy (mock-request {:model_id "10" :policy_id "999"}))]
      (is (= 404 (:status response))))))

;; ---------------------------------------------------------------------------
;; Cross-model attack scenario
;; ---------------------------------------------------------------------------

(deftest cross-model-perspective-access-blocked
  ;; Simulates: attacker knows perspective_id 50 exists on model 10,
  ;; tries to access it via their own model route /models/20/perspectives/50
  (with-redefs [persp/get-perspective       (fn [pid]
                                              (when (= pid 50)
                                                {:perspective_id 50 :model_id 10
                                                 :name "sensitive-finance-view"
                                                 :spec {:entities ["revenue"]}}))
                persp/ensure-perspective-tables! (fn [])]
    (let [response (routes/get-perspective (mock-request {:model_id "20" :perspective_id "50"}))]
      (is (= 404 (:status response))
          "Perspective on model 10 must not be accessible via model 20 route"))))

(deftest cross-model-rls-delete-blocked
  ;; Simulates: attacker tries to delete an RLS policy on model 10
  ;; via their own model route /models/20/rls/30
  (with-redefs [gov/get-rls-policy      (fn [pid]
                                          (when (= pid 30)
                                            {:policy_id 30 :model_id 10
                                             :entity "drivers" :column_name "region"}))
                gov/delete-rls-policy!   (fn [pid] (throw (ex-info "Should not be called" {})))
                gov/ensure-governance-tables! (fn [])]
    (let [response (routes/delete-rls-policy (mock-request {:model_id "20" :policy_id "30"}))]
      (is (= 404 (:status response))
          "RLS policy on model 10 must not be deletable via model 20 route"))))
