(ns diffusion.jena-test
    (:use diffusion.jena
          [diffusion.rdf :only [new-graph]]
          midje.sweet)
    (:require [plaza.rdf.core :as prc])
    (:import (diffusion.jena JenaDataSource) 
             (plaza.rdf.implementations.jena JenaModel)))
          
(fact "An empty datasource has no named-graphs"
    (names (build-datasource)) => empty?)
    
(fact "If we add named graphs to a dataset we can gey them back"
    (names (-> (build-datasource)
               (add-named-model "uri1" (prc/build-model))
               (add-named-model "uri2" (prc/build-model))
               (add-named-model "uri3" (prc/build-model)))) => (just "uri1" "uri2" "uri3" :in-any-order))
               
(fact "We can set and get the default model"
    (let [model (prc/build-model)
          datasource (-> (build-datasource)
                         (set-default-model model))]
        (prc/to-java (default-model datasource)) => (prc/to-java model)))

(fact "We can set and get a named graph"
    (let [model (prc/build-model)
          datasource (-> (build-datasource)
                         (add-named-model "uri" model))]
        (prc/to-java (named-model datasource "uri")) => (prc/to-java model)
        (contains-named-model datasource "uri")  => truthy))

(fact "We can remove a named graph"
    (-> (build-datasource)
        (add-named-model "uri" (prc/build-model))
        (remove-named-model "uri")
        (contains-named-model "uri")) => falsey)

(fact "We can insert into the default graph"
    (let [[only & _] (-> (build-datasource)
                         (direct-update "PREFIX dummy: <http://dummy.org/>
                                         INSERT DATA {dummy:a dummy:b dummy:c}")
                         (direct-query "SELECT ?s ?p ?o WHERE { ?s ?p ?o . }"))]
             (prc/qname-prefix (:?s only)) => "http://dummy.org/"
             (prc/qname-prefix (:?p only)) => "http://dummy.org/"
             (prc/qname-prefix (:?o only)) => "http://dummy.org/"
             (prc/qname-local (:?s only)) => "a"
             (prc/qname-local (:?p only)) => "b"
             (prc/qname-local (:?o only)) => "c"))
             
(fact "We can insert into any graph"
    (let [[only & _] (-> (build-datasource)
                         (add-named-model "http://dummy.org/g" (prc/build-model))
                         (direct-update "PREFIX dummy: <http://dummy.org/>
                                         INSERT DATA INTO dummy:g
                                         {dummy:a dummy:b dummy:c}")
                         (direct-query "PREFIX dummy: <http://dummy.org/>
                                        SELECT ?s ?p ?o 
                                        WHERE { GRAPH dummy:g { ?s ?p ?o . }}"))]
              (prc/qname-prefix (:?s only)) => "http://dummy.org/"
              (prc/qname-prefix (:?p only)) => "http://dummy.org/"
              (prc/qname-prefix (:?o only)) => "http://dummy.org/"
              (prc/qname-local (:?s only)) => "a"
              (prc/qname-local (:?p only)) => "b"
              (prc/qname-local (:?o only)) => "c"))
