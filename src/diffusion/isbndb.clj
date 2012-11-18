(ns diffusion.isbndb
  (:require [clojure.contrib.http.agent :as http]
            [clojure.contrib.io :as io]
            [clojure.xml :as xml]))

(def isbn-key "2WQLFX6Q")

(def retrieved-books (atom {}))

(defn get-info-by-isbn [isbn]
    (-> (http/http-agent (str "http://isbndb.com/api/books.xml?access_key=" isbn-key
                                           "&index1=isbn"
                                           "&value1=" isbn))
        http/stream
        xml/parse))

(defn retrieve-info-by-isbn [isbn]
    (http/http-agent (str "http://isbndb.com/api/books.xml?access_key=" isbn-key
                          "&results=subjects&results=details"
                          "&index1=isbn"
                          "&value1=" isbn)
                     :handler (fn [agnt] (let [tree (-> agnt http/stream xml/parse)]
                                 (println "Hola")
                                 (swap! retrieved-books assoc isbn tree)))))

(defn retrieve-info-by-isbns [isbns]
    (doseq [isbn isbns] (retrieve-info-by-isbn isbn)))

(comment
    (get-info-by-isbn "1935182595")
    (retrieve-info-by-isbns ["1935182595" "1430272317" "1934356336" "1935182641"])
    @retrieved-books
)