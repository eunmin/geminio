# Geminio

http-kit(http://http-kit.org) with consul(https://www.consul.io).

## Usage

```clojure
(require '[geminio.core :as http])

(def lb (http/create-load-balancer {:agent-host "localhost"
                                   :datacenter "dc1"
                                   :service-name "myservice"
                                   :server-list-refresh-interval-mills 30000}))

(http/get lb "/")

(http/get lb "/" {:timeout 200})
```

## License

Copyright © 2018 Eunmin Kim

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
