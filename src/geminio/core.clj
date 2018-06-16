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

(defn- create-config [{:keys [deployment-context-based-vip-addresses
                              force-client-port-configuration
                              max-auto-retries
                              max-auto-retries-next-server
                              retry-on-all-operations
                              request-specific-retry-on
                              enable-prime-connections
                              max-connections-per-host
                              max-total-connections
                              secure
                              connect-timeout
                              read-timeout
                              connection-manager-timeout
                              follow-redirects
                              connection-pool-cleaner-task-enabled
                              conn-idle-evict-time-milli-seconds
                              connection-cleaner-repeat-interval-mills
                              gzip-content-encoding-filter-enabled
                              proxy-host
                              proxy-port
                              key-store
                              key-store-password
                              trust-store
                              trust-store-password
                              client-auth-required
                              custom-ssl-socket-factory-class-name
                              hostname-validation-required
                              ignore-user-token-in-connection-pool-for-secure-client
                              load-balancer-enabled
                              server-list-refresh-interval-mills
                              zone-affinity-enabled
                              zone-exclusivity-enabled
                              prioritize-vip-address-based-servers
                              target-region]}]
  (let [^IClientConfig$Builder builder (IClientConfig$Builder/newBuilder)]
    (.withDefaultValues builder)
    (when deployment-context-based-vip-addresses
      (.withDeploymentContextBasedVipAddresses builder deployment-context-based-vip-addresses))
    (when force-client-port-configuration
      (.withForceClientPortConfiguration builder force-client-port-configuration))
    (when max-auto-retries
      (.withMaxAutoRetries builder max-auto-retries))
    (when max-auto-retries-next-server
      (.withMaxAutoRetriesNextServer builder max-auto-retries-next-server))
    (when retry-on-all-operations
      (.withRetryOnAllOperations builder retry-on-all-operations))
    (when request-specific-retry-on
      (.withRequestSpecificRetryOn builder request-specific-retry-on))
    (when enable-prime-connections
      (.withEnablePrimeConnections builder enable-prime-connections))
    (when max-connections-per-host
      (.withMaxConnectionsPerHost builder max-connections-per-host))
    (when max-total-connections
      (.withMaxTotalConnections builder max-total-connections))
    (when secure
      (.withSecure builder secure))
    (when connect-timeout
      (.withConnectTimeout builder connect-timeout))
    (when read-timeout
      (.withReadTimeout builder read-timeout))
    (when connection-manager-timeout
      (.withConnectionManagerTimeout builder connection-manager-timeout))
    (when follow-redirects
      (.withFollowRedirects builder follow-redirects))
    (when connection-pool-cleaner-task-enabled
      (.withConnectionPoolCleanerTaskEnabled builder connection-pool-cleaner-task-enabled))
    (when conn-idle-evict-time-milli-seconds
      (.withConnIdleEvictTimeMilliSeconds builder conn-idle-evict-time-milli-seconds))
    (when connection-cleaner-repeat-interval-mills
      (.withConnectionCleanerRepeatIntervalMills builder connection-cleaner-repeat-interval-mills))
    (when gzip-content-encoding-filter-enabled
      (.withGZIPContentEncodingFilterEnabled builder gzip-content-encoding-filter-enabled))
    (when proxy-host
      (.withProxyHost builder proxy-host))
    (when proxy-port
      (.withProxyPort builder proxy-port))
    (when key-store
      (.withKeyStore builder key-store))
    (when key-store-password
      (.withKeyStorePassword builder key-store-password))
    (when trust-store
      (.withTrustStore builder trust-store))
    (when trust-store-password
      (.withTrustStorePassword builder trust-store-password))
    (when client-auth-required
      (.withClientAuthRequired builder client-auth-required))
    (when custom-ssl-socket-factory-class-name
      (.withCustomSSLSocketFactoryClassName builder custom-ssl-socket-factory-class-name))
    (when hostname-validation-required
      (.withHostnameValidationRequired builder hostname-validation-required))
    (when ignore-user-token-in-connection-pool-for-secure-client
      (.ignoreUserTokenInConnectionPoolForSecureClient builder ignore-user-token-in-connection-pool-for-secure-client))
    (when load-balancer-enabled
      (.withLoadBalancerEnabled builder load-balancer-enabled))
    (when server-list-refresh-interval-mills
      (.withServerListRefreshIntervalMills builder server-list-refresh-interval-mills))
    (when zone-affinity-enabled
      (.withZoneAffinityEnabled builder zone-affinity-enabled))
    (when zone-exclusivity-enabled
      (.withZoneExclusivityEnabled builder zone-exclusivity-enabled))
    (when prioritize-vip-address-based-servers
      (.prioritizeVipAddressBasedServers builder prioritize-vip-address-based-servers))
    (when target-region
      (.withTargetRegion builder target-region))
    (.build builder)))

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
