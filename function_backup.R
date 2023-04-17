


# MY FUNCTIONS



library(shiny)
library(shinythemes)
library(shinydashboard)
library(DT)
library(tidyverse)
#library(SPARQLchunks)

library(curl)
library(httr)
library(stringi)



proxy_url <- curl::ie_get_proxy_for_url(endpoint)
proxy_config <- use_proxy(url=proxy_url)




get_all_datasets <- function(endpoint){
  
  proxy_url <- curl::ie_get_proxy_for_url(endpoint)
  proxy_config <- use_proxy(url=proxy_url)
  query <- paste0("
SELECT distinct ?graph ?dataset ?title ?class1 ?class2 ?class3 ?class4 
#SELECT DISTINCT ?graph
WHERE {
  
  
  GRAPH ?graph {
  VALUES ?all {<http://schema.org/Dataset> <http://www.w3.org/ns/dcat#Dataset> <http://rdfs.org/ns/void#Dataset> <https://schema.ld.admin.ch/LindasDataset>}
    ?dataset a ?all.}
  
  ?dataset ?p ?o.
  ?dataset <http://purl.org/dc/terms/title> ?title. #Herer gibts ein grosses Problem. Gewisse Datensets haben Schema:Name oder gar kein Titel... :(
  
  OPTIONAL {
    VALUES ?class1 {<http://schema.org/Dataset>}
  ?dataset a ?class1.}
  
  OPTIONAL {
    VALUES ?class2 {<http://www.w3.org/ns/dcat#Dataset>}
  ?dataset a ?class2.}
  
  OPTIONAL {
    VALUES ?class3 {<http://rdfs.org/ns/void#Dataset>}
  ?dataset a ?class3.}
  
  OPTIONAL {
    VALUES ?class4 {<https://schema.ld.admin.ch/LindasDataset>}
  ?dataset a ?class4.}

  #filter(lang(?title)='de')
  
}") #wwe serach for the graph
  
  querymanual <- paste(endpoint, "?", "query", "=", gsub("\\+", "%2B", URLencode(query, reserved = TRUE)), "", sep = "")
  querymanual
  queryres_csv <- GET(querymanual,proxy_config, timeout(60), add_headers(c(Accept = "text/csv")))
  queryres_csv$content <- stri_encode(queryres_csv$content, from="UTF-8",to="UTF-8")
  queryres_content_csv <-  queryres_csv$content %>% textConnection() %>% read.csv
  df <- as_tibble(queryres_content_csv)
  
  titles <- df
  
  
}


all_newest_datasets <- function(endpoint){
  
  
  # getting all graphs and datasets and titles
  datasets <- get_all_datasets(endpoint)
  
  datasets <- unique(datasets$dataset)
  
  datasets <- data.frame(datasets)
  
  datasets
  
  
  
  datasets$version <- str_extract(datasets$datasets, ".\\d$") #we get the last digit. sometimes it is more than 9 that means it has actually 2 digits
  datasets$version <-  gsub("\\/", "", datasets$version) #sometimes we get a "\" we fix that
  datasets$version <- as.numeric(datasets$version) #we want it to be numbers
  datasets$version <- ifelse(is.na(datasets$version),1,datasets$version) #na means 1
  datasets$version
  datasets
  
  
  
  all_datasets <- datasets #We want to know which datasets are the newest one and then we select them in the df above ("datasets")
  all_datasets$original_ds <- datasets$datasets
  
  # lets goooo --------------------------------------------------------------
  #which datasets are in dcat-non-compliance
  
  
  
  #not all Datasets have a version number at the end. some have "/". And we only want the newest version so wwe want to group by the same dataset 
  all_datasets$datasets <- gsub("\\/$", "", all_datasets$datasets)
  
  
  all_datasets$datasets <- gsub("\\/[0-9]{1,2}$", "", all_datasets$datasets)
  
  
  
  top_ <- all_datasets %>% group_by(datasets) %>% top_n(1)
  
  top_
  
  
}


get_data <- function( query,my_dataset){
  
  query <- paste0(
    "PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT * WHERE {
	<",my_dataset,"> ?URI ?o
}")
  
  
  querymanual <- paste(endpoint, "?", "query", "=", gsub("\\+", "%2B", URLencode(query, reserved = TRUE)), "", sep = "")
  querymanual
  queryres_csv <- GET(querymanual,proxy_config, timeout(60), add_headers(c(Accept = "text/csv")))
  queryres_csv$content <- stri_encode(queryres_csv$content, from="UTF-8",to="UTF-8")
  queryres_content_csv <-  queryres_csv$content %>% textConnection() %>% read.csv 
  # df <- as_tibble(queryres_content_csv)
  # df$s <- my_dataset
  # print(df)
  df <- as_tibble(queryres_content_csv)
  df$s <- my_dataset
  print(df)
  
  # f <- my_dataset[1,1]
  # #f <- mutate(s = s$datasets)
  # 
  # df$s <- f
  # #print(df)
  #I add here the "subject" to later being able to convert it easily to triples
}




# Metadata_pipeline_function Compares the data ----------------------------
Metadata_Pipeline <- function(df, my_dataset, endpoint){
  
  # endpoint <- "https://lindas.admin.ch/query"
  
  #df <- total_rescription %>% mutate(s$)
  
  # total_rescription$s[1]
  
  # 
  # total_rescription$s
  # df <-  total_rescription
  df$s <- unname(unlist( df$s[1]))
  # df <- total_rescription
  
  df_untouched <- df
  
  # Harmonisierung von schema.org und DCAT ----------------------------------
  
  
  # 
  # if(!("http://purl.org/dc/terms/title" %in% df$URI)) { #we want to override schema.name if DCAT is not also available
  #   df$URI <- ifelse(df$URI=="http://schema.org/name", "http://purl.org/dc/terms/title", df$URI)}
  # 
  # if(!("http://purl.org/dc/terms/description" %in% df$URI)) {
  #   df$URI <- ifelse(df$URI=="http://schema.org/description", "http://purl.org/dc/terms/description", df$URI)}
  # 
  
  
  
  df_reference <- read_csv2("manual_wanted_properties.csv")
  
  
  
  #df_reference <- df_reference[1:5]
  
  #df_reference$s <- 
  
  df_reference
  df
  
  df_join <- left_join(df_reference,df, by=c("Property"= "URI"))
  
  df_join
  df_join$s
  
  # if(5-sum(df_join$Property=="http://purl.org/dc/terms/title")>0){
  #   for( i in 1:(5-sum(df_join$Property=="http://purl.org/dc/terms/title"))) { #It is 5-sum() because 1:1 still is 1 and gives one addtional row of "Title". As a NUdge to give a title for every language
  #     df_join <- add_row(df_join, df_join[df_join$Property=="http://purl.org/dc/terms/title",][1,])
  #   }
  # }
  # 
  # if(5-sum(df_join$Property=="http://purl.org/dc/terms/description")>0){
  #   for( i in 1:(5-sum(df_join$Property=="http://purl.org/dc/terms/description"))) { #It is 4-sum() because 1:1 still is 1 and gives one addtional row of "description" 
  #     df_join <- add_row(df_join, df_join[df_join$Property=="http://purl.org/dc/terms/description",][1,])
  #   }
  # }
  # 
  print("madeithere")
  
  df_untouched <- df_untouched %>% mutate(Property = URI) %>% select(-URI)
  
  df_combined <- df_join %>% add_row(df_untouched[!(df_untouched$Property %in% df_join$Property),])
  df_combined
  
  
  df_combined <- unique(df_combined)
  
  
  print("madeithere2")
  
  
  
  # Downloading BlankNodes --------------------------------------------------
  
  blank_nodes <- df_combined$o
  blank_nodes <- str_match(blank_nodes,"^_:genid.+")
  blank_nodes <- blank_nodes[!(is.na(blank_nodes))]
  blank_nodes
  
  if(length(blank_nodes)>0) {
    query <- paste0("
                PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT * WHERE {

	<", blank_nodes[1],"> ?Property ?o. 
  
  
}")
    querymanual <- paste(endpoint, "?", "query", "=", gsub("\\+", "%2B", URLencode(query, reserved = TRUE)), "", sep = "")
    querymanual
    queryres_csv <- GET(querymanual,proxy_config, timeout(60), add_headers(c(Accept = "text/csv")))
    queryres_csv$content <- stri_encode(queryres_csv$content, from="UTF-8",to="UTF-8")
    blank_nodes_content <-  queryres_csv$content %>% textConnection() %>% read.csv #This downloads the first blank node. And it creates "Blank_nodes_content" to be used in the for loop below
    blank_nodes_content$s <- blank_nodes[1]
    length(blank_nodes_content$s)
    
    if(length(blank_nodes)>1) {
      
      for(i in 2:length(blank_nodes)){
        query <- paste0("
                PREFIX schema: <http://schema.org/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT * WHERE {

	<", blank_nodes[i],"> ?Property ?o.
  
  
}")
        querymanual <- paste(endpoint, "?", "query", "=", gsub("\\+", "%2B", URLencode(query, reserved = TRUE)), "", sep = "")
        querymanual
        queryres_csv <- GET(querymanual,proxy_config, timeout(60), add_headers(c(Accept = "text/csv")))
        queryres_csv$content <- stri_encode(queryres_csv$content, from="UTF-8",to="UTF-8")
        queryres_content_csv <-  queryres_csv$content %>% textConnection() %>% read.csv
        queryres_content_csv$s <- blank_nodes[i]
        blank_nodes_content <- add_row(blank_nodes_content, queryres_content_csv) 
        
        
      }}
    
    f <- blank_nodes_content
    f
    blank_nodes_content <-   blank_nodes_content %>% mutate(Publisher = NA, Datatype=NA, ReqLevel =NA, Explanation = NA, num_min_max_entries = NA)
    blank_nodes_content
    
    df_combined <- add_row(df_combined,blank_nodes_content)
    df_combined
    
  }
  print("madeithere4")
  
  # adding the blank_Nodes to the rest of the df ----------------------------
  
  
  # df_combined <- unique(df_combined)
  df_combined
  df_combined$ReqLevel <- ifelse(is.na(df_combined$ReqLevel),"not_DCAT",df_combined$ReqLevel)
  
  print("madeitherefastfertig5")
  
  df_combined <- df_combined %>% mutate(level= (ifelse(ReqLevel=="Mandatory", 1, ifelse(ReqLevel=="Recommended",2,ifelse(ReqLevel=="not_DCAT",3,ifelse(ReqLevel=="Optional",4, NA) )))))
  
  df_combined <-df_combined[order(df_combined$level),]
  
  df_combined <- df_combined %>% select(-level)
  
  df_combined <- df_combined %>%  relocate(o) %>% relocate(Property) %>%  relocate(s)
  
  df_combined <- df_combined %>% rename(Subject = s, Predicate =Property,Object = o, Cardinality = num_min_max_entries )
  
  df_combined <- df_combined %>% mutate(dataset_name = df_untouched$s[1])
  
  df_combined 
  
}
