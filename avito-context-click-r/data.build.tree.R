##############################################################
## create tree features data
##############################################################
cat("Tree data... \n")

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/build_tree')
  
  cat('\nMerging data.all.search.small + data.all.search.cont \n')
  data.all.tree <- merge(
    data.all.search.small, 
    data.all.search.cont[ID %in% data.all.search.small$ID], 
    by="ID")
  fn.soar.unload(data.all.search.small, data.all.search.cont)
  invisible(gc()) 
  
  cat('\nAdding data.all.search.info\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.search.info[SearchID %in% unique(data.all.tree$SearchID)], 
    by="SearchID")
  fn.soar.unload(data.all.search.info)
  invisible(gc())
  
  cat('\nAdding data.all.search.info.cont\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.search.info.cont[SearchID %in% unique(data.all.tree$SearchID)], 
    by="SearchID")
  fn.soar.unload(data.all.search.info.cont)
  invisible(gc())
  
  cat('\nAdding data.all.prob.1way\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.prob.1way, 
    by="ID")
  fn.soar.unload(data.all.prob.1way)
  invisible(gc())
  
  cat('\nAdding data.all.prob.2way.ad.us\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.prob.2way.ad.us, 
    by="ID")
  fn.soar.unload(data.all.prob.2way.ad.us)
  invisible(gc())
  
  cat('\nAdding data.all.prob.2way.ad.srch\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.prob.2way.ad.srch, 
    by="ID")
  fn.soar.unload(data.all.prob.2way.ad.srch)
  invisible(gc())
  
  cat('\nAdding data.all.prob.2way.us.srch\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.prob.2way.us.srch, 
    by="ID")
  fn.soar.unload(data.all.prob.2way.us.srch)
  invisible(gc())
  
  cat('\nAdding data.all.prob.2way.srch\n')
  data.all.tree <- merge(
    data.all.tree, 
    data.all.prob.2way.srch, 
    by="ID")
  fn.soar.unload(data.all.prob.2way.srch)
  invisible(gc())
  
  cat('\nRemoving unwanted columns\n')
  cols.excl <- c(
    "UserID", "UserIPID", "UserAgentID", "UserDeviceID",
    "AdID", "AdParams",
    "SearchLocID"
  )
  
  for (col.nam in cols.excl) {
    data.all.tree[, col.nam := NULL, with=F]
  }
  invisible(gc())
  
  cat('\nSorting columns\n')
  setkeyv(data.all.tree, c("SearchDate", "SearchID", "Position"))
  invisible(gc())
  
  
  cat('\nFilling NAs\n')
  cols.extra <- c("ID", "SearchID", "SearchType", "IsClick")
  cols.in <- sort(setdiff(colnames(data.all.tree), cols.extra))
  
  for (col.nam in cols.in) {
    if (any(is.na(data.all.tree[[col.nam]]))) {
      setnames(data.all.tree, col.nam, 'change_val')
      data.all.tree[is.na(change_val), change_val := -1]
      setnames(data.all.tree, 'change_val', col.nam)
    }
  }
  
  invisible(gc())
  cols.in.tree <- cols.in
  cat('\nSaving\n')
  setcolorder(data.all.tree, c(cols.extra, cols.in))
  Store(data.all.tree, cols.in)
  invisible(gc())
  
  
  data.l2.pred <- fn.load.ens(
    ens.cols = c(
      "data.ftrl.04.pred",
      "data.ftrl.05.pred",
      "data.ftrl.06.pred",
      "data.fm.05.pred",
      "data.fm.04.pred",
      "data.fm.03.pred",
      "data.fm.02.pred",
      "data.fm.01.pred"
    ), print.err = F)
  data.l2.all.tree <- merge(data.all.tree, data.l2.pred,
                            by="ID")
  rm(data.l2.pred)
  fn.soar.unload(data.all.tree)
  
  col.num <- sapply(data.l2.all.tree, is.numeric)
  col.num <- names(col.num)[col.num]
  col.num <- col.num[(col.num %like% '(^Prob)|(^ftrl)|(^fm)|(^Ratio)|AdHistCTR')]
  
  for (col.nam in col.num) {
    setnames(data.l2.all.tree, col.nam, 'change_val')
    data.l2.all.tree[, change_val := round(change_val, digits = 6)]
    setnames(data.l2.all.tree, 'change_val', col.nam)
  }
  
  Store(data.l2.all.tree)
  invisible(gc())
  
  fn.clean.worker()
}
fn.kill.wk()

#############################################################
# Probability features - full dataset
#############################################################
cat("Probability features full data... \n")

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/prob_features_full')
  
  cols.in.1way <- c(
    "AdID", "AdCatID",  "AdParams",
    "UserID", "UserIPID", "UserAgentID", 
    "UserAgentOSID", "UserDeviceID", "UserAgentFamilyID",
    "SearchLocID", "SearchCatID"
  )
  data.all.prob.full.1way <- fn.build.prob.full(cols.in.1way)
  Store(data.all.prob.full.1way)
  invisible(gc())
  
  cols.in.2way <- c(
    "AdID", "AdCatID",  "AdParams",
    "UserID", "UserIPID", "UserAgentID", 
    "UserAgentOSID", "UserDeviceID", "UserAgentFamilyID",
    "SearchLocID", "SearchCatID"
  )
  
  data.all.prob.full.2way.ad.us <- fn.build.prob.full(
    fn.build.interaction(cols.in.2way, c("Ad", "Us")))
  Store(data.all.prob.full.2way.ad.us)
  invisible(gc())
  
  data.all.prob.full.2way.ad.srch <- fn.build.prob.full(
    fn.build.interaction(cols.in.2way, c("Ad", "Search")))
  Store(data.all.prob.full.2way.ad.srch)
  invisible(gc())
  
  data.all.prob.full.2way.srch <- fn.build.prob.full(
    fn.build.interaction(cols.in.2way, c("Search", "Search")))
  invisible(gc())
  Store(data.all.prob.full.2way.srch)
  invisible(gc())
  
  cols.in.2way.us.srch.1 <- c(
    "UserAgentID", "UserAgentOSID", "UserDeviceID", "UserAgentFamilyID",
    "SearchLocID", "SearchCatID"
  )
  
  data.all.prob.full.2way.us.srch.1 <- fn.build.prob.full(
    fn.build.interaction(cols.in.2way.us.srch.1, c("Us", "Search")))
  invisible(gc())
  Store(data.all.prob.full.2way.us.srch.1)
  invisible(gc())
  
  cols.in.2way.us.srch.2 <- c(
    "UserID", "UserIPID", 
    "SearchLocID", "SearchCatID"
  )
  
  data.all.prob.full.2way.us.srch.2 <- fn.build.prob.full(
    fn.build.interaction(cols.in.2way.us.srch.2, c("Us", "Search")))
  invisible(gc())
  Store(data.all.prob.full.2way.us.srch.2)
  invisible(gc())
  
  
  
  cat("\nMerging probabilities\n")
  data.all.prob.full <- data.all.prob.full.1way
  setkey(data.all.prob.full, ID)
  
  setkey(data.all.prob.full.2way.ad.us, ID)
  fn.check.id(data.all.prob.full, 
              data.all.prob.full.2way.ad.us)
  for (col.nam in colnames(data.all.prob.full.2way.ad.us)[-1]) {
    data.all.prob.full[
      , col.nam := data.all.prob.full.2way.ad.us[[col.nam]], 
      with=F]
  }
  fn.soar.unload(data.all.prob.full.2way.ad.us)
  invisible(gc())
  
  
  
  setkey(data.all.prob.full.2way.ad.srch, ID)
  fn.check.id(data.all.prob.full, 
              data.all.prob.full.2way.ad.srch)
  for (col.nam in colnames(data.all.prob.full.2way.ad.srch)[-1]) {
    data.all.prob.full[
      , col.nam := data.all.prob.full.2way.ad.srch[[col.nam]], 
      with=F]
  }
  fn.soar.unload(data.all.prob.full.2way.ad.srch)
  invisible(gc())
  
  
  
  setkey(data.all.prob.full.2way.us.srch.1, ID)
  fn.check.id(data.all.prob.full, 
              data.all.prob.full.2way.us.srch.1)
  for (col.nam in colnames(data.all.prob.full.2way.us.srch.1)[-1]) {
    data.all.prob.full[
      , col.nam := data.all.prob.full.2way.us.srch.1[[col.nam]], 
      with=F]
  }
  fn.soar.unload(data.all.prob.full.2way.us.srch.1)
  invisible(gc())
  
  
  setkey(data.all.prob.full.2way.us.srch.2, ID)
  fn.check.id(data.all.prob.full, 
              data.all.prob.full.2way.us.srch.2)
  for (col.nam in colnames(data.all.prob.full.2way.us.srch.2)[-1]) {
    data.all.prob.full[
      , col.nam := data.all.prob.full.2way.us.srch.2[[col.nam]], 
      with=F]
  }
  fn.soar.unload(data.all.prob.full.2way.us.srch.2)
  invisible(gc())
  
  
  
  setkey(data.all.prob.full.2way.srch, ID)
  fn.check.id(data.all.prob.full, 
              data.all.prob.full.2way.srch)
  for (col.nam in colnames(data.all.prob.full.2way.srch)[-1]) {
    data.all.prob.full[
      , col.nam := data.all.prob.full.2way.srch[[col.nam]], 
      with=F]
  }
  fn.soar.unload(data.all.prob.full.2way.srch)
  invisible(gc())
  
  
  cat("\nSaving dataset csv\n")
  setkey(data.all.prob.full, ID)
  data.all.prob.full[, ID := NULL]
  fn.write.csv.chunk(data=data.all.prob.full,
                     file=fn.out.file("data.all.prob.full.csv"),
                     compress=F)
  
  fn.clean.worker()
}
fn.kill.wk()



##############################################################
## full tree mdel data
##############################################################
tic()
cat("full tree model data... \n")


fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
  
  fn.init.worker('data_build/build_tree_full')
  
  
  cat('\nMerging data.all.search.info + data.all.search.info.cont \n')
  data.all.tree.full <- merge(data.all.search.info, 
                              data.all.search.info.cont, 
                              by="SearchID")
  fn.soar.unload(data.all.search.info, data.all.search.info.cont)
  invisible(gc()) 
  
  setkey(data.all.tree.full, "SearchID")
  data.all.tree.full <- data.all.tree.full[J(data.all.search$SearchID)]
  if (!all(data.all.tree.full$SearchID == data.all.search$SearchID)) {
    stop('SearchIDs do not match')
  }
  data.all.tree.full[, ID := data.all.search$ID]
  
  fn.to.data.all.tree.full <- function(data.add) {
    fn.check.id(data.all.tree.full, data.add)
    for (col.nam in colnames(data.add)) {
      if (col.nam %ni% colnames(data.all.tree.full)) {
        data.all.tree.full[, col.nam := data.add[[col.nam]], with=F]
      }
    }
    invisible(NULL)
  }
  
  cat('\nAdding data.all.search\n')
  fn.to.data.all.tree.full(data.all.search)
  fn.soar.unload(data.all.search)
  
  cat('\nAdding data.all.search.cont\n')
  fn.to.data.all.tree.full(data.all.search.cont)
  fn.soar.unload(data.all.search.cont)
  
  setkeyv(data.all.tree.full, c("SearchDate", "SearchID", "Position"))
  
  #   cat('\nRemoving unwanted columns\n')
  #   cols.excl <- c(
  #     "UserID", "UserIPID", "UserAgentID", "UserDeviceID",
  #     "AdID", "AdParams", "SearchLocID"
  #   )
  #   
  #   for (col.nam in cols.excl) {
  #     data.all.tree.full[, col.nam := NULL, with=F]
  #   }
  #   invisible(gc())
  
  cat('\nSorting columns\n')
  setkeyv(data.all.tree.full, c("SearchDate", "SearchID", "Position"))
  invisible(gc())
  
  
  cat('\nFilling NAs\n')
  cols.extra <- c("ID", "SearchID", "SearchType", "IsClick")
  cols.in <- sort(setdiff(colnames(data.all.tree.full), cols.extra))
  
  for (col.nam in cols.in) {
    if (any(is.na(data.all.tree.full[[col.nam]]))) {
      setnames(data.all.tree.full, col.nam, 'change_val')
      data.all.tree.full[is.na(change_val), change_val := -1]
      setnames(data.all.tree.full, 'change_val', col.nam)
    }
  }
  
  invisible(gc())
  
  cat('\nSaving\n')
  setcolorder(data.all.tree.full, c(cols.extra, cols.in))
  Store(data.all.tree.full)
  invisible(gc())
  
  setkey(data.all.tree.full, ID)
  cat("\nSaving dataset csv\n")
  fn.write.csv.chunk(data=data.all.tree.full,
                     file=fn.out.file("data.all.tree.full.csv"),
                     compress=F)
  
  cat('\nMerging data\n')
  system(
    paste(
      "paste -d ',' ",
      fn.out.file("data.all.tree.full.csv"),
      fn.out.file("data.all.prob.full.csv"),
      "> ", fn.out.file("data.all.tree.full.merge.csv")
    )
  )
  cat('\nCompressing data\n')
  system(
    paste(
      "pigz -f", fn.out.file("data.all.tree.full.merge.csv")
    )
  )
  
  cat('\nSaving libsvm data\n')
  cols.in.tree.full <- c(
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
    "UserQryTotalTime","ProbAdID","ProbAdCatID","ProbAdParams",
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
    "ProbSearchLocIDSearchCatID")
  extra.tr.sel <- "int(row[\"SearchOrdUsrDesc\"]) <= 10 and" # row[\"SearchDate\"]) >= 1431396000
  system(paste(
    "cd ../avito-context-click-py &&", 
    "pypy -u convert_csv_to_libsvm.py", 
    "-input_files ../data/output-r/data.all.tree.full.merge.csv",
    "-out_selector '{", 
    "\"../data/output-r/data.val.tr.full.libsvm\": lambda file, row: ", extra.tr.sel, " row[\"SearchType\"] in [\"hist\", \"tr\"],",   
    "\"../data/output-r/data.val.tt.full.libsvm\": lambda file, row: row[\"SearchType\"] in [\"val\"],",
    "\"../data/output-r/data.test.tr.full.libsvm\": lambda file, row: ", extra.tr.sel, " row[\"SearchType\"] in [\"hist\", \"tr\", \"val\"],", 
    "\"../data/output-r/data.test.tt.full.libsvm\": lambda file, row: row[\"SearchType\"] in [\"test\"]",
    "}'",
    "-weight_builder_dict '{", 
    "\"../data/output-r/data.val.tr.full.libsvm\": lambda file, row: 1/(float(row[\"SearchOrdUsrDesc\"])),",   
    "\"../data/output-r/data.test.tr.full.libsvm\": lambda file, row: 1/(float(row[\"SearchOrdUsrDesc\"]))", 
    "}'",
    "-feat_map_file ../data/output-r/data.all.full.fmap",
    "-col_out IsClick",
    "-col_in_num", paste(cols.in.tree.full, collapse=' '),
    "-missing_values ''  'na'  'nan' 'NA' 'NaN' '-1'",
    ">> ../data/log/data_build/build_tree_full.log 2>&1"))
  
  fn.clean.worker()
}
fn.kill.wk()





##############################################################
## Ensenble cross validation scheme
##############################################################
tic()
cat("Ensenble cross validation... \n")

data.cv.ens <- fn.cv.ens.folds()
Store(data.cv.ens)

toc()
