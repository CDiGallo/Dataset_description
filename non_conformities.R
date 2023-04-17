
# datasetdescription ------------------------------------------------------

#choose here INT or PROD

endpoint <- "https://lindas.admin.ch/query"


#loading functions and depenencies

source("function.r")



#############################################################################
# Getting the newest Datasets ---------------------------------------------

#############################################################################


#for one dataset there can be a lot of versions. We only want the newest one.


datasets <- all_newest_datasets(endpoint)




#############################################################################
# Getting the Data ----------------------------------------------------------

#############################################################################



#get the description  for the first dataset
total_rescription <- get_data(endpoint, datasets[1,1])

total_rescription

#for the first descriptions we also want all the subnodes. Secondly we want to compare it to a number of properties which come from DCAT in the file "manual_wanted_properties.csv".

total_rescription <- Metadata_Pipeline(total_rescription,unname(unlist( total_rescription$s[1,1])),endpoint)
total_rescription
# 
df <- total_rescription




#############################################################################
# Looping trhough all datasets --------------------------------------------

#############################################################################


for( i in 2:length(datasets$original_ds)) {
  
  temp <- get_data(endpoint, datasets[i,1])
  #unname(unlist(temp$s[1,1]))
  
  temp
  total_rescription
  
  temp <- Metadata_Pipeline(temp,unname(unlist( temp$s[i,1])),endpoint)
  
 #all the datasetsdesciptions are added together. Which row belongs to which datasets is signified in the "dataset_name" column
  total_rescription <-  add_row(total_rescription,temp)
  
}
  


total_rescription



df <- total_rescription

unique(df$dataset_name)



#############################################################################
# we want all datasets which are not compliant either with mandato --------

#############################################################################

df
non_conforming <- df %>% filter( is.na(Object), ReqLevel != "Optional")  #%>% select(dataset_name) %>% unique()
non_conforming
non_conforming %>% group_by(Predicate) %>% count()

non_conforming %>% group_by(dataset_name) %>% count()


