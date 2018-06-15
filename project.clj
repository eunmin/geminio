(defproject geminio "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [http-kit "2.2.0"]
                 [io.reactivex/rxjava "1.0.9"]
                 [com.netflix.ribbon/ribbon "2.2.4"]
                 [com.netflix.ribbon/ribbon-loadbalancer "2.2.4"]
                 [com.ecwid.consul/consul-api "1.3.0"]])
