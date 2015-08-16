# #############################################################
# # merge lucas and dmitry data
# #############################################################
# 
tic()
cat("Merging datasets... \n")

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/combine_datasets')

  cat("\nLoading datasets\n")
  load(fn.rdata.file('data.reduced.all.RData'))

  setkey(data.reduced.all, ID)
  setkey(data.l2.all.tree, ID)
  
  cat("\nCalculating common cols\n")
  col.common <- intersect(colnames(data.reduced.all),
                          colnames(data.l2.all.tree))
  col.common <- unique(c(col.common, "HistCTR", "Price"))
  
  col.uniq.dtry <- sapply(data.reduced.all, 
                          function(x) length(unique(x)))
  col.uniq.dtry <- col.uniq.dtry[!names(col.uniq.dtry) %in% 
                                   col.common]
  
  col.uniq.lucas <- sapply(data.l2.all.tree, 
                          function(x) length(unique(x)))
  col.uniq.lucas <- col.uniq.lucas[
    !names(col.uniq.lucas) %in%
      col.common]
  
  # check for length and then for value
  cols.match <- list()
  for (ix in 1:length(col.uniq.lucas)) {
    col.uniq <- col.uniq.lucas[ix]
    col.same <- col.uniq.dtry[col.uniq.dtry == col.uniq]
    if (length(col.same) >= 1) {
      col.lucas.nam <- names(col.uniq)
      for (col.dtry.nam in names(col.same)) {
        if (all(data.reduced.all[[col.dtry.nam]] == 
                 data.l2.all.tree[[col.lucas.nam]])) {
          cols.match[[col.lucas.nam]] <- col.dtry.nam
          col.common <- unique(c(col.common, col.dtry.nam))
        }
      }
    }
  }
  
  cat("\nCopying and cols and saving RData\n")
  for (col.nam in setdiff(col.common, "ID")) {
    data.reduced.all[, col.nam := NULL, with=F]
  }
  
  data.all.tree.dl <- data.l2.all.tree
  fn.soar.unload(data.l2.all.tree)
  
  setkey(data.reduced.all, ID)
  setkey(data.all.tree.dl, ID)
  
  for (col.nam in setdiff(colnames(data.reduced.all), "ID")) {
    data.all.tree.dl[, col.nam := data.reduced.all[[col.nam]], with=F]
    data.reduced.all[, col.nam := NULL, with=F]
    invisible(gc())
  }
  rm(data.reduced.all)
  
  save(data.all.tree.dl, file=fn.rdata.file('data.all.tree.dl.RData'))
  
  cat("\nSaving dataset csv\n")
  fn.write.csv.chunk(data=data.all.tree.dl,
                     file=fn.out.file("data.all.tree.dl.csv"),
                     compress=F)
  
  cols.extra <- c("ID", "SearchID", "SearchType", "IsClick")
  cols.in <- sort(setdiff(colnames(data.all.tree.dl), 
                          c(cols.extra)))
  
  cols.in.combine <- cols.in
  Store(cols.in.combine)
  
  rm(data.all.tree.dl)
  invisible(gc())
  
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libsvm.py", 
    "-input_files ../data/output-r/data.all.tree.dl.csv",
    "-out_selector '{", 
    "\"../data/output-libsvm/data.val.tr.libsvm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\"],",   
    "\"../data/output-libsvm/data.val.tt.libsvm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libsvm/data.test.tr.libsvm\": lambda file, row: row[\"SearchType\"] in [\"hist\", \"tr\", \"val\"],", 
    "\"../data/output-libsvm/data.test.tt.libsvm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-col_out IsClick",
    "-col_in_num", paste(cols.in.combine, collapse=' '),
    "-missing_values ''  'na'  'nan' 'NA' 'NaN' '-1'",
    ">> ../data/log/data_build/combine_datasets.log 2>&1"))
  
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libsvm.py", 
    "-input_files ../data/output-r/data.all.tree.dl.csv",
    "-out_selector '{", 
    "\"../data/output-libsvm/data.val.tr.nllh.libsvm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",   
    "\"../data/output-libsvm/data.val.tt.nllh.libsvm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libsvm/data.test.tr.nllh.libsvm\": lambda file, row: row[\"SearchType\"] in [\"val\"],", 
    "\"../data/output-libsvm/data.test.tt.nllh.libsvm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-feat_map_file ../data/output-libsvm/data.all.nllh.fmap",
    "-col_out IsClick",
    "-col_in_num", paste(cols.in.combine[!cols.in.combine %like% 'likeli'], 
                         collapse=' '),
    "-missing_values ''  'na'  'nan' 'NA' 'NaN' '-1'",
    ">> ../data/log/data_build/combine_datasets.log 2>&1"))
  
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libsvm.py", 
    "-input_files ../data/output-r/data.all.tree.dl.csv",
    "-out_selector '{", 
    "\"../data/output-libsvm/data.val.tr.nprob.libsvm\": lambda file, row: row[\"SearchType\"] in [\"tr\"],",   
    "\"../data/output-libsvm/data.val.tt.nprob.libsvm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libsvm/data.test.tr.nprob.libsvm\": lambda file, row: row[\"SearchType\"] in [\"tr\", \"val\"],", 
    "\"../data/output-libsvm/data.test.tt.nprob.libsvm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-weight_builder_dict '{", 
    "\"../data/output-libsvm/data.val.tr.nprob.libsvm\": lambda file, row: 1/(float(row[\"SearchOrdUsrDesc\"])-3),",   
    "\"../data/output-libsvm/data.test.tr.nprob.libsvm\": lambda file, row: 1/(float(row[\"SearchOrdUsrDesc\"])-0)", 
    "}'",
    "-feat_map_file ../data/output-libsvm/data.all.nprob.fmap",
    "-col_out IsClick",
    "-col_in_num", paste(cols.in.combine[!cols.in.combine %like% '^Prob'], 
                         collapse=' '),
    "-missing_values ''  'na'  'nan' 'NA' 'NaN' '-1'",
    ">> ../data/log/data_build/combine_datasets.log 2>&1"))
  
    load(fn.rdata.file('data.full.all.RData'))
    setkey(data.full.all, ID)
    data.full.all[, ID := NULL]
    cat("\nSaving dataset csv\n")
    fn.write.csv.chunk(data=data.full.all,
                       file=fn.out.file("data.dtry.full.all.csv"),
                       compress=F)
    rm(data.full.all)
    invisible(gc())
#     
    cat('\nMerging data\n')
    system(
      paste(
        "paste -d ',' ",
        fn.out.file("data.all.tree.full.csv"),
        fn.out.file("data.all.prob.full.csv"),
        fn.out.file("data.dtry.full.all.csv"),
        "> ", fn.out.file("data.all.tree.full.combine.csv")
      )
    )
    #cat('\nCompressing data\n')
    #system(
    #  paste(
    #    "pigz -f", fn.out.file("data.all.tree.full.combine.csv")
    #  )
    #)
    
  cat('\nSaving libsvm data\n')
  cols.in.combine.full <- c(
    "AdCatID","AdHistCTR","AdID","AdParams","AdPrice",
    "AdTitleSZ","CountAdSearch","CountAdSearchCat",
    "CountAdSearchLoc","CountAdUsers","CountIPUser",
    "CountUserAd","CountUserAdDupT1","CountUserAdDupT3",
    "CountUserAdT1","CountUserAdT3","CountUserSearch",
    "CountUserSearchCategory","CountUserSearchLocation",
    "Position","RatioAdPos1","RatioSearchRuss","SearchAdCount",
    "SearchAdT1Count","SearchAdT2Count","SearchAdT3Count",
    "SearchCatID","SearchDate","SearchLocID","SearchOrdUsrAsc",
    "SearchOrdUsrDesc","SearchParamsSZ","SearchQuerySZ",
    "SearchRussian","UserAgentFamilyID","UserAgentID",
    "UserAgentOSID","UserDeviceID","UserID","UserIPID",
    "UserLogged","UserPrevPhoneRequest",
    "UserPrevPrevPrevQryDate","UserPrevPrevQryDate",
    "UserPrevQryDate","UserPrevVisitReq","UserPrevVisitReqUni",
    "UserQryTotalTime",
    "ProbAdID","ProbAdCatID","ProbAdParams",
    "ProbUserID","ProbUserIPID","ProbUserAgentID",
    "ProbUserAgentOSID","ProbUserDeviceID",
    "ProbUserAgentFamilyID","ProbSearchLocID",
    "ProbSearchCatID","ProbAdCatIDUserAgentFamilyID",
    "ProbAdIDUserAgentFamilyID","ProbAdCatIDUserAgentOSID",
    "ProbAdIDUserAgentOSID","ProbAdCatIDUserID","ProbAdIDUserID",
    "ProbAdCatIDUserIPID","ProbAdIDUserIPID",
    "ProbAdCatIDSearchCatID","ProbAdIDSearchCatID",
    "ProbAdCatIDSearchLocID","ProbAdIDSearchLocID",
    "ProbSearchCatIDUserAgentFamilyID",
    "ProbSearchLocIDUserAgentFamilyID",
    "ProbSearchCatIDUserAgentOSID",
    "ProbSearchLocIDUserAgentOSID","ProbSearchCatIDUserID",
    "ProbSearchLocIDUserID","ProbSearchCatIDUserIPID",
    "ProbSearchLocIDUserIPID",
    'SearchDayYear', 'SearchPosition2Count', 'SearchPosition6Count', 
    'SearchPosition7Count', 'AdPosition1Count', 'AdPosition7Count', 
    'SearchParamsCount', 'LocationUserUniqueCount', 'CategoryUserUniqueCount', 
    'SearchIDPreviousAge', 'AdParamsSize', 'AdParamsCount', 'UserAdCount', 
    'AdCategoryPriceDeviation', 'UserAdViewTotalCount', 'UserAdViewUniqueCount', 
    'UserAdCategoryPriceMean', 'UserAdCategoryPriceMedian', 
    'UserAdCategoryPriceMin', 'UserAdCategoryPriceMax', 'UserAdViewTotalCount2', 
    'UserAdViewUniqueCount2', 'UserAdCategoryPriceMean2', 
    'UserAdCategoryPriceMedian2', 'UserAdCategoryPriceMin2', 
    'UserAdCategoryPriceMax2'
  )

  extra.tr.sel <- "int(row[\"SearchOrdUsrDesc\"]) <= 7 and" # row[\"SearchDate\"]) >= 1431396000
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libsvm.py", 
    "-input_files ../data/output-r/data.all.tree.full.combine.csv",
    "-out_selector '{", 
    "\"../data/output-libsvm/data.val.tr.full.libsvm\": lambda file, row: ", extra.tr.sel, " row[\"SearchType\"] in [\"hist\", \"tr\"],",   
    "\"../data/output-libsvm/data.val.tt.full.libsvm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-libsvm/data.test.tr.full.libsvm\": lambda file, row: ", extra.tr.sel, " row[\"SearchType\"] in [\"hist\", \"tr\", \"val\"],", 
    "\"../data/output-libsvm/data.test.tt.full.libsvm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-feat_map_file ../data/output-libsvm/data.all.full.fmap",
    "-col_out IsClick",
    "-col_in_num", paste(unique(cols.in.combine.full), collapse=' '),
    "-missing_values ''  'na'  'nan' 'NA' 'NaN' '-1'",
    ">> ../data/log/data_build/combine_datasets.log 2>&1"))
  
  
  cat("\nDone!\n")
  
  fn.clean.worker()
}
fn.kill.wk()

