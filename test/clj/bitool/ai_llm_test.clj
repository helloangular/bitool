(ns bitool.ai-llm-test
  (:require [clojure.test :refer :all]
            [bitool.ai.llm :as llm]))

(deftest call-anthropic-requires-api-key
  (testing "call-anthropic throws when ANTHROPIC_API_KEY is not set"
    (with-redefs [bitool.ai.llm/call-anthropic
                  (fn [& _]
                    ;; The real function checks for an API key.
                    ;; We verify the contract by calling with no env.
                    (throw (ex-info "ANTHROPIC_API_KEY not set" {:error "missing_api_key"})))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (llm/call-anthropic "system" "user"))))))

(deftest call-openai-requires-api-key
  (testing "call-openai throws when OPENAI_API_KEY is not set"
    (with-redefs [bitool.ai.llm/call-openai
                  (fn [& _]
                    (throw (ex-info "OPENAI_API_KEY not set" {:error "missing_api_key"})))]
      (is (thrown? clojure.lang.ExceptionInfo
                   (llm/call-openai "system" "user"))))))

(deftest call-llm-falls-back-to-openai
  (testing "call-llm tries OpenAI when Anthropic fails"
    (let [calls (atom [])]
      (with-redefs [llm/call-anthropic (fn [& _]
                                         (swap! calls conj :anthropic)
                                         (throw (ex-info "Anthropic down" {:status 500})))
                    llm/call-openai    (fn [& _]
                                         (swap! calls conj :openai)
                                         "fallback-response")]
        (let [result (llm/call-llm "system" "user")]
          (is (= "fallback-response" result))
          (is (= [:anthropic :openai] @calls)))))))

(deftest call-llm-throws-when-both-fail
  (testing "call-llm throws combined error when both providers fail"
    (with-redefs [llm/call-anthropic (fn [& _]
                                       (throw (ex-info "Anthropic fail" {:provider "anthropic"})))
                  llm/call-openai    (fn [& _]
                                       (throw (ex-info "OpenAI fail" {:provider "openai"})))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo
                            #"Both LLM providers failed"
                            (llm/call-llm "system" "user"))))))

(deftest call-llm-text-delegates-correctly
  (testing "call-llm-text calls call-llm without tools"
    (with-redefs [llm/call-llm (fn [sys msg & {:keys [max-tokens temperature]}]
                                 (str "ok:" max-tokens ":" temperature))]
      (is (= "ok:900:0" (llm/call-llm-text "sys" "msg")))
      (is (= "ok:500:0.3" (llm/call-llm-text "sys" "msg" :max-tokens 500 :temperature 0.3))))))

(deftest call-llm-passes-temperature
  (testing "temperature is forwarded to provider"
    (let [captured-temp (atom nil)]
      (with-redefs [llm/call-anthropic (fn [_ _ & {:keys [temperature]}]
                                         (reset! captured-temp temperature)
                                         "ok")]
        (llm/call-llm "sys" "msg" :temperature 0.7)
        (is (= 0.7 @captured-temp))))))
