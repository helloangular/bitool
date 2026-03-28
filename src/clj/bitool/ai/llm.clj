(ns bitool.ai.llm
  "Shared LLM provider layer — Anthropic-first with OpenAI fallback.
   Extracted from pipeline.intent to be reused by ai.assistant and pipeline.intent."
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [bitool.config :refer [env]]))

;; ---------------------------------------------------------------------------
;; API key helpers
;; ---------------------------------------------------------------------------

(defn- anthropic-api-key []
  (or (get env :anthropic-api-key)
      (System/getenv "ANTHROPIC_API_KEY")))

(defn- openai-api-key []
  (or (get env :openai-api-key)
      (System/getenv "OPENAI_API_KEY")))

;; ---------------------------------------------------------------------------
;; Anthropic provider
;; ---------------------------------------------------------------------------

(defn call-anthropic
  "Call Anthropic Messages API.

   Options:
     :model       — model id (default \"claude-opus-4-6\")
     :max-tokens  — response budget (default 4096)
     :temperature — sampling temperature (default 0)
     :tools       — vector of Anthropic tool definitions (optional)
     :tool-choice — tool_choice map (optional)

   When tools are provided, returns the :input of the first tool_use block.
   When no tools, returns the text content as a string."
  [system-prompt user-message & {:keys [model max-tokens temperature tools tool-choice]
                                  :or   {model "claude-opus-4-6"
                                         max-tokens 4096
                                         temperature 0}}]
  (let [api-key (anthropic-api-key)
        _       (when-not api-key
                  (throw (ex-info "ANTHROPIC_API_KEY not set" {:error "missing_api_key"})))
        body    (cond-> {:model       model
                         :max_tokens  max-tokens
                         :temperature temperature
                         :system      system-prompt
                         :messages    [{:role "user" :content user-message}]}
                  tools       (assoc :tools tools)
                  tool-choice (assoc :tool_choice tool-choice))
        resp    (http/post "https://api.anthropic.com/v1/messages"
                           {:headers          {"x-api-key"         api-key
                                               "anthropic-version" "2023-06-01"
                                               "content-type"      "application/json"}
                            :body             (json/generate-string body)
                            :throw-exceptions false})
        status  (:status resp)
        resp-body (try (json/parse-string (:body resp) true)
                       (catch Exception _ (:body resp)))]
    (when (>= status 400)
      (let [err-type (get-in resp-body [:error :type] "unknown")
            err-msg  (get-in resp-body [:error :message] (str "Anthropic HTTP " status))]
        (log/error "Anthropic API error" {:status status :model model :type err-type :error err-msg})
        (throw (ex-info (str "Anthropic error (" err-type "): " err-msg)
                        {:status status :provider "anthropic" :error resp-body}))))
    (let [content (:content resp-body)]
      (if tools
        ;; Tool-use mode: return the input of the first tool_use block
        (let [tool-use (first (filter #(= "tool_use" (:type %)) content))]
          (when-not tool-use
            (throw (ex-info "Anthropic did not return tool_use" {:content content})))
          (:input tool-use))
        ;; Text mode: return concatenated text blocks
        (->> content
             (filter #(= "text" (:type %)))
             (map :text)
             (clojure.string/join "\n"))))))

;; ---------------------------------------------------------------------------
;; OpenAI provider
;; ---------------------------------------------------------------------------

(defn- openai-function-from-tool
  "Convert an Anthropic-style tool definition to OpenAI function calling format."
  [tool]
  {:type "function"
   :function {:name        (:name tool)
              :description (:description tool)
              :parameters  (:input_schema tool)}})

(defn call-openai
  "Call OpenAI Chat Completions API.

   Options:
     :model       — model id (default \"gpt-4.1\")
     :max-tokens  — response budget (default 4096)
     :temperature — sampling temperature (default 0)
     :tools       — vector of Anthropic-style tool definitions (converted automatically)
     :tool-choice — tool_choice map for forced function call (optional)

   When tools are provided, returns parsed arguments of the first matching tool call.
   When no tools, returns the assistant message content as a string."
  [system-prompt user-message & {:keys [model max-tokens temperature tools tool-choice]
                                  :or   {model "gpt-4.1"
                                         max-tokens 4096
                                         temperature 0}}]
  (let [api-key (openai-api-key)
        _       (when-not api-key
                  (throw (ex-info "OPENAI_API_KEY not set" {:error "missing_api_key"})))
        body    (cond-> {:model       model
                         :max_tokens  max-tokens
                         :temperature temperature
                         :messages    [{:role "system" :content system-prompt}
                                       {:role "user"   :content user-message}]}
                  tools       (assoc :tools (mapv openai-function-from-tool tools))
                  tool-choice (assoc :tool_choice tool-choice))
        resp    (http/post "https://api.openai.com/v1/chat/completions"
                           {:headers          {"Authorization" (str "Bearer " api-key)
                                               "content-type"  "application/json"}
                            :body             (json/generate-string body)
                            :throw-exceptions false})
        status  (:status resp)
        resp-body (try (json/parse-string (:body resp) true)
                       (catch Exception _ (:body resp)))]
    (when (>= status 400)
      (let [err-msg (or (get-in resp-body [:error :message])
                        (str "OpenAI HTTP " status))]
        (log/error "OpenAI API error" {:status status :model model :error err-msg})
        (throw (ex-info (str "OpenAI error: " err-msg)
                        {:status status :provider "openai" :error resp-body}))))
    (let [message (get-in resp-body [:choices 0 :message])]
      (if tools
        ;; Function-call mode
        (let [tool-calls (:tool_calls message)
              tool-call  (first tool-calls)]
          (when-not tool-call
            (throw (ex-info "OpenAI did not return function call" {:message message})))
          (json/parse-string (get-in tool-call [:function :arguments]) true))
        ;; Text mode
        (or (:content message) "")))))

;; ---------------------------------------------------------------------------
;; Tool-choice format conversion
;; ---------------------------------------------------------------------------

(defn- anthropic->openai-tool-choice
  "Convert Anthropic-style tool_choice to OpenAI function-calling format.
   Anthropic: {:type \"tool\" :name \"fn_name\"}
   OpenAI:    {:type \"function\" :function {:name \"fn_name\"}} or \"required\""
  [tc]
  (cond
    (nil? tc)            nil
    (= "tool" (:type tc)) {:type "function" :function {:name (:name tc)}}
    (= "any" (:type tc))  "required"
    (= "auto" (:type tc)) "auto"
    :else                  tc))

;; ---------------------------------------------------------------------------
;; Fallback chain
;; ---------------------------------------------------------------------------

(defn call-llm
  "Try Anthropic first, fall back to OpenAI if Anthropic fails.
   Accepts all options that call-anthropic and call-openai accept.

   For tool-use mode, pass :tools and :tool-choice.
   For text mode, omit :tools — returns plain text."
  [system-prompt user-message & {:keys [temperature max-tokens tools tool-choice
                                         anthropic-model openai-model]
                                  :or   {temperature 0 max-tokens 4096}}]
  (try
    (call-anthropic system-prompt user-message
                    :model       (or anthropic-model "claude-opus-4-6")
                    :max-tokens  max-tokens
                    :temperature temperature
                    :tools       tools
                    :tool-choice tool-choice)
    (catch Exception e
      (log/warn "Anthropic failed, falling back to OpenAI"
                {:anthropic_error (.getMessage e)
                 :provider        "anthropic"
                 :status          (some-> (ex-data e) :status)})
      (try
        (call-openai system-prompt user-message
                     :model       (or openai-model "gpt-4.1")
                     :max-tokens  max-tokens
                     :temperature temperature
                     :tools       tools
                     :tool-choice (anthropic->openai-tool-choice tool-choice))
        (catch Exception e2
          (log/error "Both LLM providers failed"
                     {:anthropic_error (.getMessage e)
                      :openai_error    (.getMessage e2)
                      :openai_status   (some-> (ex-data e2) :status)})
          (throw (ex-info (str "Both LLM providers failed. Anthropic: " (.getMessage e)
                               " | OpenAI: " (.getMessage e2))
                          {:anthropic (ex-data e)
                           :openai    (ex-data e2)})))))))

;; ---------------------------------------------------------------------------
;; Text-mode convenience (for assistant tasks — no tool schema needed)
;; ---------------------------------------------------------------------------

(defn call-llm-text
  "Call LLM in text mode (no tool use). Returns a plain string.
   Uses Anthropic first with OpenAI fallback.

   Options:
     :max-tokens  — response budget (default 900)
     :temperature — sampling temperature (default 0)"
  [system-prompt user-message & {:keys [max-tokens temperature]
                                  :or   {max-tokens 900 temperature 0}}]
  (call-llm system-prompt user-message
            :max-tokens  max-tokens
            :temperature temperature))
