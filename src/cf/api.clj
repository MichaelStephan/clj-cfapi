(ns cf.api
  (:require [org.httpkit.client :as http]
            [clojure.data.json :as json]
            [taoensso.timbre :as log]
            [clojure.core.async :as async :refer [chan timeout go >! close!]])
  (:use [slingshot.slingshot :only [try+ throw+]]))

(def http-timeout 5000)
(def http-options {:keepalive -1 :timeout http-timeout :insecure? true})

(defrecord Client [url access_token refresh_token])

(defn- error? [data]
  (and
    (coll? data)
    (= :api/error (first data))))

(def success? (complement error?)) 

(defmacro e<! [body]
  (let [data (gensym)]
    `(let [~data (<! (~@body))]
       (if (success? ~data)
         ~data
         (throw+ (second ~data))))))

(defmacro step [id ret-ch & body]
  (let [msg (gensym) e (gensym) error (gensym)]
    `(do (go
           (>! ~ret-ch (try+
                         ~@body
                         (catch Object ~e
                           (let [~msg (str ~id " failed")
                                 ~error {:type :step-error :message ~msg :cause ~e}]
                             (log/error ~msg ~error)
                             [:api/error ~error]))))
           (close! ~ret-ch))
         ~ret-ch)))

(defn- body->json [body]
  (try+
    (json/read-str body :key-fn keyword)
    (catch Object e
      (throw+ {:type :json-parse-error
               :cause e}))))

(defn- make-http-response-fn [url ret-ch]
  (fn [{:keys [status headers body error opts]}]
    (step (str "Processing http response from " url) ret-ch
          (when error
            (throw+ {:type ::unexpected-error
                     :message (str "An error occured " error)}))
          (when (not= 200 status)
            (throw+ {:type ::unexpected-server-response
                     :message (str "Server sent unexpected response code " status)}))
          (body->json body))))

(defn info [api-url]
  (let [ret-ch (chan)
        url (str api-url "/v2/info")]
    (http/get url http-options (make-http-response-fn url ret-ch))
    ret-ch))

(defn- token [uaa-url username password]
  (let [ret-ch (chan)
        url (str uaa-url "/oauth/token")]
    (http/post url (assoc http-options
                          :headers {"AUTHORIZATION" "Basic Y2Y6"}
                          :query-params {:username username :password password :grant_type "password"})
               (make-http-response-fn url ret-ch))
    ret-ch))

(defn make-client [url username password]
  (let [ret-ch (chan)]
    (step "Make client" ret-ch
          (let [{:keys [token_endpoint]} (e<! (info url))
                {:keys [access_token refresh_token]} (e<! (token token_endpoint username password))]
            (Client. url access_token refresh_token)))
    ret-ch))

(defn- http-get-page [{:keys [access_token] :as client} url page page-size]
  (let [ret-ch (chan)]
    (http/get url (assoc http-options
                         :headers {"AUTHORIZATION" (str "Bearer " access_token)}
                         :query-params {:order-direction "asc" :page page :results-per-page page-size})
              (make-http-response-fn url ret-ch))
    ret-ch))

(defn- http-get-all-pages [{:keys [access_token] :as client} url page-size]
  (let [ret-ch (chan)]
    (step "Fetching Spaces" ret-ch
          (let [{:keys [total_results]} (e<! (http-get-page client url 1 1))
                total-pages (Math/ceil (/ total_results page-size))]
            (for [page (range 1 (inc total-pages))]
              (http-get-page client url page page-size))))
    ret-ch))

(defn spaces [{:keys [url] :as client}]
  (http-get-all-pages client (str url "/v2/spaces") 1))

(def client (async/<!! (make-client "https://api.us-east.cf.yaas.io/" "x" "y")))

(->>
  (spaces client)
  (async/<!!)
  (async/merge)
  (async/into [])
  (async/<!!)
  (map :next_url)
  (count)
  println)
