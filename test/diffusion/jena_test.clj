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
        
