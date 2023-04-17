
# Which Datasets are outdated? --------------------------------------------

endpoint <- "https://lindas.admin.ch/query"


source(function.r)


all_df <- get_all_datasets(endpoint)

all_df

true_df <- all_newest_datasets(endpoint)

true_df


superfluos_df <- all_df$dataset[!(all_df$dataset %in% true_df$original_ds)]

unique(superfluos_df)


print(paste("these datasets should not be in PROD because they are deprecated:",unique(superfluos_df)))


nrow(unique(true_df))+nrow(unique(superfluos_df))-nrow(unique(all_df))


unique(all_df$dataset)

