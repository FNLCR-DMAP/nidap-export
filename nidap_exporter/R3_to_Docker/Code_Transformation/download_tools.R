# Dev Note
# The download_tools.R was developed by Robin, and refactored by Rui
# Current directory is 
# /rstudio-files/ccbr-data/git-repos/nidap-export
# This is the refactored version, original file can be found in 
# /rstudio-files/ccbr-data/RSK/nidap_transfer_scripts2/

# Library
library(RCurl)
library(httr)
library(arrow)
library(dplyr)

# report_differences function, use dplyr::all_equal 
report_differences <- function(target, new){
  if(class(target) != "data.frame"){
    return("Not a dataframe, skipping")
    
  }else{
    rownames(new) <- NULL
    rownames(target) <- NULL
    names(new) <- gsub("\\.", "_", names(new))
    colnames(new) <- gsub("-", ".", colnames(new))
    Comparison_results <- dplyr::all_equal(target %>%  
                                             mutate_if(is.numeric, round, 7) %>%  
                                             mutate_if(is.factor, as.character), 
                                           new %>% mutate_if(is.numeric, round, 7) %>% 
                                             mutate_if(is.factor, as.character),
                                           ignore_col_order = TRUE,
                                           ignore_row_order = TRUE)
    return(Comparison_results)
  }
}

# figure_out_nidao_files is used to format retrieved file from nidap into rds
figure_out_nidap_files <- function(transfers){
  input_file <- transfers
  
  if(sum(grepl(".parquet$", input_file))){
    print("Nidap datset")
    fin0 <- input_file[grepl(".parquet$", input_file)]
    infos = file.info(fin0)
    df_total <- data.frame()
    
    for(infoi in rownames(infos)){
      info <- infos[infoi, ]
      
      if(info$size > 0){
        df_raw <- read_parquet(infoi)
        df <- data.frame(df_raw)
        df_total <- rbind(df_total, df)
      }
    }
    
    return(df_total)
    
  }else if(length(input_file) == 1 && sum(grepl(".csv$", input_file))){
    return(read.csv(input_file, 
                    stringsAsFactors = FALSE, 
                    header = TRUE, 
                    check.names = FALSE))
    
  }else if(sum(grepl("abc$", input_file))){
    return(read.csv(input_file, 
                    stringsAsFactors = FALSE, 
                    header = TRUE, 
                    check.names = FALSE))
    
  }else if(sum(grepl(".rds$", input_file))){
    print("R rds object, readRDS")
    return(readRDS(input_file))
    
  }else if(sum(grepl(".h5$", input_file))){
    print("R4 h5 method")
    output_list <- c()
    for(item in input_file){
      output_list <- c(output_list,item)
    }
    return(output_list)
    
  }else{
    fraw <- data.frame(input_file, stringsAsFactors = FALSE)
    colnames(fraw) <- "value"
    return (fraw)
  }
}

# pullnidap_raw is used to acquire dataset from NIDAP
pullnidap_raw <- function(key, rid, branch){
  dirdown <- "nidap_downloads"
  dir.create(dirdown, showWarnings = FALSE)
  
  # NIDAP address 
  url = "https://nidap.nih.gov/"
  dataproxy = "foundry-data-proxy/api/dataproxy/datasets/"
  catalog = "foundry-catalog/api/catalog/datasets/"
  callurl <- paste0(url, catalog, rid, 
                    '/views2/', branch, '/files?pageSize=100')
  rm <- GET(url = callurl, 
            add_headers(Authorization = paste("Bearer", key, sep = " ")), 
            verify = FALSE, content_type_json())
  con <- content(rm, "parsed")
  cont <- content(rm, "text")
  files = c()
  filenum = 0
  
  # Loop through parsed content
  for(content_line in con$values){
    filenum = filenum + 1
    file_url = paste0(url, dataproxy, rid, "/transactions/",
                      content_line$transactionRid, "/", 
                      content_line$logicalPath)
    paths <- strsplit(content_line$logicalPath, "/")[[1]]
    file_folder_directory = paths[length(paths)]
    filename <- paste0(dirdown, "/", file_folder_directory)
    
    # Acquired from transaction
    rm3 <- GET(url = file_url, 
               add_headers(Authorization = paste("Bearer", key, sep = " ")), 
               verify = FALSE, write_disk(filename, overwrite = TRUE))
    info = file.info(filename)
    print(filename)
    files = c(files, filename)
  }
  return(files)
}

# pullnidap_dataset is more complicated than pullnidap_raw, not sure what this is used in
pullnidap_dataset <- function(key, rid, branch){
  
  # NIDAP address
  url = "https://nidap.nih.gov/"
  dataproxy = "foundry-data-proxy/api/dataproxy/datasets/"
  catalog = "foundry-catalog/api/catalog/datasets/"
  callurl <- paste0(url, catalog, rid, 
                    '/reverse-transactions2/', branch, '?pageSize=100')
  rm <- GET(url = callurl, 
            add_headers(Authorization = paste("Bearer", key, sep = " ")), 
            verify = FALSE, content_type_json())
  con <- content(rm, "parsed")
  cont <- content(rm, "text")
  jsonlite::prettify(content(rm, "text"))
  ts = ""
  
  #find last committed transaction
  for(iv in 1:length(con$values)){
    
    if(con$values[[iv]]$transaction$status == "COMMITTED")
      
      if(ts == ""){
        ts = "COMMITTED"
        v = iv
      }
    
    if(con$values[[iv]]$transaction$closeTime > 
       con$values[[v]]$transaction$closeTime){
      v <- iv
    }
  }
  
  if(ts == ""){
    print("no committed transaction")
  }
  
  i <- con$values[[v]]$rid
  call = paste0(url, catalog, rid, 
                '/transactions/', i, '/files/paged2?pageSize=100')
  rm2 <- GET(url = call, 
             add_headers(Authorization = paste("Bearer", key, sep = " ")), 
             verify = FALSE)
  con2 <- content(rm2, "parsed")
  df_total = data.frame()
  filenum = 0
  for(parsed_line in con2$values){
    file_url = paste0(url, dataproxy, rid, 
                      "/transactions/", i, "/", parsed_line$logicalPath)
    filename = paste0(filenum, "_test.txt")
    
    if(grepl(".parquet", parsed_line$logical)){
      rm3 <- GET(url = file_url, 
                 add_headers(Authorization = paste("Bearer", 
                                                   key, sep = " ")), 
                 verify = FALSE, 
                 write_disk(filename, overwrite = TRUE))
      info = file.info(filename)
      
      if(info$size > 0){
        df1 <- read_parquet(filename)
        df <- data.frame(df1)
        df_total <- rbind(df_total, df)
      }
    }
  }
  
  return(df_total)
}

pullnidap_raw_R4 <- function(key, rid, branch){
  dirdown <- "nidap_downloads"
  dir.create(dirdown, showWarnings = FALSE)
  
  # NIDAP address 
  url = "https://nidap.nih.gov/"
  dataproxy = "foundry-data-proxy/api/dataproxy/datasets/"
  catalog = "foundry-catalog/api/catalog/datasets/"
  callurl <- paste0(url, catalog, rid, 
                    '/views2/', branch, '/files?pageSize=100')
  rm <- GET(url = callurl, 
            add_headers(Authorization = paste("Bearer", key, sep = " ")), 
            verify = FALSE, content_type_json())
  con <- content(rm, "parsed")
  cont <- content(rm, "text")
  files = c()
  filenum = 0
  
  # Loop through parsed content
  for(content_line in con$values){
    filenum = filenum + 1
    file_url = paste0(url, dataproxy, rid, "/transactions/",
                      content_line$transactionRid, "/", 
                      content_line$logicalPath)
    paths <- strsplit(content_line$logicalPath, "/")[[1]]
    file_folder_directory = paths[length(paths)]
    filename <- paste0(dirdown, "/", file_folder_directory)
    
    # Acquired from transaction
    rm3 <- GET(url = file_url, 
               add_headers(Authorization = paste("Bearer", key, sep = " ")), 
               verify = FALSE, write_disk(filename, overwrite = TRUE))
    info = file.info(filename)
    print(filename)
    files = c(files, filename)
  }
  return(files)
}
