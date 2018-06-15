(ns geminio.core
  (:refer-clojure :exclude [get])
  (:require [org.httpkit.client :as http])
  (:import [com.ecwid.consul.v1
            ConsulClient
            QueryParams
            QueryParams$Builder
            Response]
           [com.ecwid.consul.v1.health.model HealthService]
           [com.netflix.client DefaultLoadBalancerRetryHandler]
           [com.netflix.loadbalancer.reactive
            LoadBalancerCommand
            ServerOperation]
           [com.netflix.loadbalancer
            Server
            ServerList
            ILoadBalancer
            LoadBalancerBuilder
            PollingServerListUpdater]
           [com.netflix.client.config IClientConfig$Builder]
           [rx Observable]))

(defn- create-config [{:keys [server-list-refresh-interval-mills]}]
  (.build (doto (IClientConfig$Builder/newBuilder)
            (.withDefaultValues)
            (.withServerListRefreshIntervalMills server-list-refresh-interval-mills))))

(defn- create-server [host port]
  (Server. host port))

(defn- get-server-list [^String agent-host ^String datacenter ^String service-name]
  (let [^ConsulClient client (ConsulClient. agent-host)
        ^QueryParams query-params (.build (.setDatacenter (QueryParams$Builder/builder) datacenter))]
    (map
     (fn [^HealthService service]
       (create-server (.getAddress (.getNode service)) (.getPort (.getService service))))
     (.getValue (.getHealthServices client service-name true query-params)))))

(defn- create-server-list [agent-host datacenter service-name]
  (proxy [ServerList] []
    (getServerList []
      (get-server-list agent-host datacenter service-name))
    (getInitialListOfServers []
      (get-server-list agent-host datacenter service-name))
    (getUpdatedListOfServers []
      (get-server-list agent-host datacenter service-name))))

(defn create-load-balancer [{:keys [agent-host datacenter service-name] :as opts}]
  (.buildDynamicServerListLoadBalancerWithUpdater
   (doto (LoadBalancerBuilder/newBuilder)
     (.withServerListUpdater (PollingServerListUpdater. (create-config opts)))
     (.withDynamicServerList (create-server-list agent-host datacenter service-name)))))

(defn request [^ILoadBalancer lb {:keys [scheme path retry] :as opts :or {scheme :http
                                                                          retry {:same-server 0
                                                                                 :next-server 0
                                                                                 :enabled? false}}}]
  (let [lbc (.build (doto (LoadBalancerCommand/builder)
                      (.withLoadBalancer lb)
                      (.withRetryHandler (DefaultLoadBalancerRetryHandler.
                                          (:same-server retry)
                                          (:next-server retry)
                                          (:enabled? retry)))))]
    (.first
     (.toBlocking
      (.submit lbc (proxy [ServerOperation] []
                     (call [^Server server]
                       (try
                         (let [host (.getHost server)
                               port (.getPort server)
                               url (str (name scheme) "://" host ":" port path)]
                           (Observable/just @(http/request
                                              (-> opts
                                                  (dissoc :scheme :path)
                                                  (dissoc :retry)
                                                  (assoc :url url)))))
                         (catch Throwable t
                           (Observable/error t))))))))))

(defmacro ^:private defreq [method]
  `(defn ~method ~'[lb path & opts]
     (request ~'lb (merge ~'opts {:path ~'path :method ~(keyword method)}))))

(defreq get)
(defreq delete)
(defreq head)
(defreq post)
(defreq put)
(defreq options)
(defreq patch)
(defreq propfind)
(defreq proppatch)
(defreq lock)
(defreq unlock)
(defreq report)
(defreq acl)
(defreq copy)
(defreq move)
