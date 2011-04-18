(ns diffusion.rdf
    (:require [diffusion.jena :as dj]
              [plaza.rdf.core :as prc]
              [plaza.rdf.sparql :as prs]
              plaza.rdf.implementations.jena
              plaza.utils))

(plaza.rdf.implementations.jena/init-jena-framework)

(prc/register-rdf-ns :diffusion "http://rhizomik.net/diffusion#")
(prc/register-rdf-ns :user "http://rhizomik.net/diffusion/user/")
(prc/register-rdf-ns :item "http://rhizomik.net/diffusion/item/")
(prc/register-rdf-ns :tag "http://rhizomik.net/diffusion/tag/")
(prc/register-rdf-ns :activation "http://rhizomik.net/diffusion/activation/")
(prc/register-rdf-ns :xsd "http://www.w3.org/2001/XMLSchema#")

(defn make-user [user]
    (prc/rdf-resource :user user))

  
(defn make-item [item]
    (prc/rdf-resource :item item))
    

(defn make-tag [tag]
    (prc/rdf-resource :tag tag))


(defn user? [resource]
    (= (prc/resource-qname-prefix resource)
       (prc/find-ns-registry :user)))
    

(defn item? [resource]
    (= (prc/resource-qname-prefix resource)
       (prc/find-ns-registry :item)))
    

(defn tag? [resource]
    (= (prc/resource-qname-prefix resource)
       (prc/find-ns-registry :tag)))
    

(defn new-graph []
    (dj/build-datasource))

(defn add-user [datasource user]
    (prc/with-model datasource
        (prc/model-add-triples 
            [[(make-user user) prc/rdf:type [:diffusion :User]]]))
    datasource)

(defn add-item [datasource item]
    (prc/with-model datasource
        (prc/model-add-triples 
            [[(make-item item) prc/rdf:type [:diffusion :Item]]]))
    datasource)

 
(defn add-tag [datasource tag]
    (prc/with-model datasource
        (prc/model-add-triples 
            [[(make-tag tag) prc/rdf:type [:diffusion :Tag]]]))
    datasource)
    

(defn add-review [datasource user item]
    (let [user-resource (make-user user)
          item-resource (make-item item)]
        (add-user datasource user)
        (add-item datasource item)
        (prc/with-model datasource
            (prc/model-add-triples
                [[user-resource [:diffusion :has-reviewed] item-resource]]))
        datasource))

(defn tag-item [datasource item tag]
    (let [item-resource (make-item item)
          tag-resource (make-tag tag)]
        (add-item datasource item)
        (add-tag datasource tag)
        (prc/with-model datasource
            (prc/model-add-triples
                [[item-resource [:diffusion :has-tag] tag-resource]]))
        datasource))
    

(defn- count-relation [datasource {subject :subject property :property object :object}]
    (->> (format "PREFIX diffusion: <%1$s> 
                  PREFIX user: <%2$s>
                  PREFIX item: <%3$s>
                  PREFIX tag: <%4$s>
                  SELECT (COUNT(?elem) AS ?counter)
                  WHERE { %5$s diffusion:%6$s %7$s }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  (prc/find-ns-registry :item)
                  (prc/find-ns-registry :tag)
                  (if subject subject "?elem")
                  property
                  (if object object "?elem"))
         (dj/direct-query datasource)
         first
         :?counter
         prc/literal-value))

(defn count-reviews-of-user [datasource user]
    (count-relation datasource {:subject (format "user:%s" user) :property "has-reviewed"}))

(defn count-reviewers-of-item [datasource item]
    (count-relation datasource {:object (format "item:%s" item) :property "has-reviewed"}))

(defn count-tags-of-item [datasource item]
    (count-relation datasource {:subject (format "item:%s" item) :property "has-tag"}))

(defn count-items-with-tag [datasource tag]
    (count-relation datasource {:object (format "tag:%s" tag) :property "has-tag"}))

(defn all-of-type [datasource type]
    (->> (prs/defquery
            (prs/query-set-type :select)
            (prs/query-set-vars [:?elem])
            (prs/query-set-pattern
                (prs/make-pattern [[:?elem prc/rdf:type [:diffusion type]]])))
         (prs/model-query datasource)
         (map :?elem)
         (map prc/resource-qname-local)))
           
(defn count-of-type [datasource type]
    (->> (format "PREFIX diffusion: <%1$s>
                  SELECT (COUNT(?elem) AS ?counter) 
                  WHERE { ?elem a diffusion:%2$s }" 
                  (prc/find-ns-registry :diffusion)
                  (plaza.utils/keyword-to-string type))
         (dj/direct-query datasource)
         first
         :?counter
         prc/literal-value))
        
(defn all-users [datasource]
    (all-of-type datasource :User))
    
(defn all-items [datasource]
    (all-of-type datasource :Item))
    
(defn all-tags [datasource]
    (all-of-type datasource :Tag))

(defn- get-relations-other [datasource {subject :subject property :property object :object}]
    (->> (format "PREFIX diffusion: <%1$s> 
                  PREFIX user: <%2$s>
                  PREFIX item: <%3$s>
                  PREFIX tag: <%4$s>
                  SELECT ?elem
                  WHERE { %5$s diffusion:%6$s %7$s }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  (prc/find-ns-registry :item)
                  (prc/find-ns-registry :tag)
                  (if subject subject "?elem")
                  property
                  (if object object "?elem"))
         (dj/direct-query datasource)
         (map :?elem)
         (map prc/resource-qname-local)))
             
(defn reviewed-by [datasource item]
    (get-relations-other datasource {:property "has-reviewed" :object (format "item:%s" item)}))

(defn reviews-of [datasource user]
    (get-relations-other datasource {:subject (format "user:%s" user) :property "has-reviewed"}))
         
(defn tagged-with [datasource tag]
    (get-relations-other datasource {:property "has-tag" :object (format "tag:%s" tag)}))

(defn tags-of [datasource item]
    (get-relations-other datasource {:subject (format "item:%s" item) :property "has-tag"}))
    
(defn new-items-for [datasource user]
    (->> (format "PREFIX diffusion: <%1$s>
                  PREFIX user: <%2$s>
                  SELECT ?item
                  WHERE { ?item a diffusion:Item 
                          FILTER NOT EXISTS { user:%3$s diffusion:has-reviewed ?item } }"
                  (prc/find-ns-registry :diffusion)
                  (prc/find-ns-registry :user)
                  user)
         (dj/direct-query datasource)
         (map :?item)
         (map prc/resource-qname-local)))

;; Activations

(defn- create-namedgraph-if-needed [datasource uri]
    (if (dj/contains-named-model datasource uri)
        datasource
        (dj/add-named-model datasource uri (prc/build-model))))

 
(defn initial-activations [datasource user]
    (let [user-ns (prc/find-ns-registry :user)
          diffusion-ns (prc/find-ns-registry :diffusion)
          xsd-ns (prc/find-ns-registry :xsd)
          update0 (format "PREFIX diffusion: <%1$s>
                           PREFIX user: <%2$s>
                           PREFIX xsd: <%3$s>
                           INSERT INTO user:%4$s
                           { ?item diffusion:initial-activation \"0.0\"^^xsd:double }
                           WHERE { ?item a diffusion:Item .
                                   FILTER NOT EXISTS { user:%4$s diffusion:has-reviewed ?item } }"
                           diffusion-ns
                           user-ns
                           xsd-ns
                           user)
          update1 (format "PREFIX diffusion: <%1$s>
                           PREFIX user: <%2$s>
                           PREFIX xsd: <%3$s>
                           INSERT INTO user:%4$s
                           { ?item diffusion:initial-activation \"1.0\"^^xsd:double }
                            WHERE { user:%4$s diffusion:has-reviewed ?item }"
                           diffusion-ns
                           user-ns
                           xsd-ns
                           user)]
          (-> datasource
              (create-namedgraph-if-needed (str user-ns user))
              (dj/direct-update update0)
              (dj/direct-update update1))))
          
(defn counts-users-to-items [datasource]
    (let [activation-ns (prc/find-ns-registry :activation)
          diffusion-ns (prc/find-ns-registry :diffusion)
          sparql (format "PREFIX diffusion:<%1$s>
                          PREFIX activation:<%2$s>
                          INSERT INTO activation:counters
                          { ?user diffusion:num-items ?counter }
                          WHERE { { SELECT ?user (COUNT(distinct ?item) AS ?counter)
                                    WHERE { ?user a diffusion:User .
                                            ?user diffusion:has-reviewed ?item }
                                    GROUP BY ?user}}"
                          diffusion-ns
                          activation-ns)]
        (-> datasource
            (create-namedgraph-if-needed (str activation-ns "counters"))
            (dj/direct-update sparql))))

(defn counts-items-to-users [datasource]
    (let [activation-ns (prc/find-ns-registry :activation)
          diffusion-ns (prc/find-ns-registry :diffusion)
          sparql (format "PREFIX diffusion:<%1$s>
                          PREFIX activation:<%2$s>
                          INSERT INTO activation:counters
                          { ?item diffusion:num-users ?counter }
                          WHERE { { SELECT ?item (COUNT(distinct ?user) AS ?counter)
                                    WHERE { ?item a diffusion:Item .
                                            ?user diffusion:has-reviewed ?item }
                                    GROUP BY ?item}}"
                          diffusion-ns
                          activation-ns)]
        (-> datasource
            (create-namedgraph-if-needed (str activation-ns "counters"))
            (dj/direct-update sparql))))        

(defn counts-items-to-tags [datasource]
    (let [activation-ns (prc/find-ns-registry :activation)
          diffusion-ns (prc/find-ns-registry :diffusion)
          sparql (format "PREFIX diffusion:<%1$s>
                          PREFIX activation:<%2$s>
                          INSERT INTO activation:counters
                          { ?item diffusion:num-tags ?counter }
                          WHERE { { SELECT ?item (COUNT(distinct ?tag) AS ?counter)
                                    WHERE { ?item a diffusion:Item .
                                            ?item diffusion:has-tag ?tag }
                                    GROUP BY ?item}}"
                          diffusion-ns
                          activation-ns)]
        (-> datasource
            (create-namedgraph-if-needed (str activation-ns "counters"))
            (dj/direct-update sparql))))
     
(defn counts-tags-to-items [datasource]
    (let [activation-ns (prc/find-ns-registry :activation)
          diffusion-ns (prc/find-ns-registry :diffusion)
          sparql (format "PREFIX diffusion:<%1$s>
                          PREFIX activation:<%2$s>
                          INSERT INTO activation:counters
                          { ?tag diffusion:num-items ?counter }
                          WHERE { { SELECT ?tag (COUNT(distinct ?item) AS ?counter)
                                    WHERE { ?tag a diffusion:Tag .
                                            ?item diffusion:has-tag ?tag }
                                    GROUP BY ?tag}}"
                          diffusion-ns
                          activation-ns)]
        (-> datasource
            (create-namedgraph-if-needed (str activation-ns "counters"))
            (dj/direct-update sparql))))
            
(defn activate-users-from-items [datasource user]
    (let [diffusion-ns (prc/find-ns-registry :diffusion)
          activation-ns (prc/find-ns-registry :activation)
          user-ns (prc/find-ns-registry :user)
          sparql (format "PREFIX diffusion: <%1$s>
                          PREFIX activation: <%2$s> 
                          PREFIX user: <%3$s>
                          INSERT INTO user:%4$s
                          { ?user diffusion:from-items ?accum }
                          WHERE { SELECT ?user (SUM(?activation/?degree) AS ?accum)
                                  WHERE  { ?user a diffusion:User .
                                           ?user diffusion:has-reviewed ?item .
                                           GRAPH user:%4$s { ?item diffusion:initial-activation ?activation }
                                           GRAPH activation:counters { ?item diffusion:num-users ?degree } }
                                  GROUP BY ?user }"
                          diffusion-ns
                          activation-ns
                          user-ns
                          user)]
        (dj/direct-update datasource sparql)))
        
(defn activate-items-from-users [datasource user]
    (let [diffusion-ns (prc/find-ns-registry :diffusion)
          activation-ns (prc/find-ns-registry :activation)
          user-ns (prc/find-ns-registry :user)
          sparql (format "PREFIX diffusion: <%1$s>
                          PREFIX activation: <%2$s> 
                          PREFIX user: <%3$s>
                          INSERT INTO user:%4$s
                          { ?item diffusion:from-users ?accum }
                          WHERE { SELECT ?item (SUM(?activation/?degree) AS ?accum)
                                  WHERE { ?item a diffusion:Item .
                                          ?user diffusion:has-reviewed ?item .
                                          GRAPH user:%4$s { ?user diffusion:from-items ?activation }
                                          GRAPH activation:counters { ?user diffusion:num-items ?degree } }
                                  GROUP BY ?item }"
                          diffusion-ns
                          activation-ns
                          user-ns
                          user)]
        (dj/direct-update datasource sparql)))

        
(defn activate-tags-from-items [datasource user]
    (let [diffusion-ns (prc/find-ns-registry :diffusion)
          activation-ns (prc/find-ns-registry :activation)
          user-ns (prc/find-ns-registry :user)
          sparql (format "PREFIX diffusion: <%1$s>
                          PREFIX activation: <%2$s> 
                          PREFIX user: <%3$s>
                          INSERT INTO user:%4$s
                          { ?tag diffusion:from-items ?accum }
                          WHERE { SELECT ?tag (SUM(?activation/?degree) AS ?accum)
                                  WHERE  { ?tag a diffusion:Tag .
                                           ?item diffusion:has-tag ?tag .
                                           GRAPH user:%4$s { ?item diffusion:initial-activation ?activation }
                                           GRAPH activation:counters { ?item diffusion:num-tags ?degree } }
                                  GROUP BY ?tag }"
                          diffusion-ns
                          activation-ns
                          user-ns
                          user)]
        (dj/direct-update datasource sparql)))
        

(defn activate-items-from-tags [datasource user]
    (let [diffusion-ns (prc/find-ns-registry :diffusion)
          activation-ns (prc/find-ns-registry :activation)
          user-ns (prc/find-ns-registry :user)
          sparql (format "PREFIX diffusion: <%1$s>
                          PREFIX activation: <%2$s> 
                          PREFIX user: <%3$s>
                          INSERT INTO user:%4$s
                          { ?item diffusion:from-tags ?accum }
                          WHERE { SELECT ?item (SUM(?activation/?degree) AS ?accum)
                                  WHERE { ?item a diffusion:Item .
                                          ?item diffusion:has-tag ?tag .
                                          GRAPH user:%4$s { ?tag diffusion:from-items ?activation }
                                          GRAPH activation:counters { ?tag diffusion:num-items ?degree } }
                                  GROUP BY ?item }"
                          diffusion-ns
                          activation-ns
                          user-ns
                          user)]
        (dj/direct-update datasource sparql)))

