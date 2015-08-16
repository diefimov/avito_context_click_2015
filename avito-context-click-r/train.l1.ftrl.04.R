#############################################################
# save csv to disk data
#############################################################

fn.register.wk(1)
tmp <- foreach(tmp=1, .noexport=all.noexport) %dopar% {
    
  fn.init.worker("ftrl_04/build_csv")
  
  cat("\nLoading data...\n")
  data.all.cur <- data.all.lr
  # fn.soar.unload(data.all.lr)
  
  cols.extra <- c("ID", "SearchID", "SearchType", "IsClick", "SearchDate")
  cols.in <- setdiff(colnames(data.all.cur), cols.extra)
  
  # data.all.cur[, SearchType:=NULL]
  
  for (jx in 1:2) {
    fold.name <- paste0("ftrl_04_", jx)
  
    if (jx == 1) {
      setkeyv(data.all.cur, c("SearchDate", "SearchID", "Position"))
    } else {
      setkeyv(data.all.cur, c("Position", "SearchCatID", "SearchRussian", 
                              "SearchDate", "SearchID"))
    }
    data.search.type <- data.all.cur$SearchType
    for (test.type in c("tr", "val", "test")) {
      cat("\nTest type", test.type, jx, "...\n")
      
      data.fold <- fn.create.data.fold(fold.name, test.type)
      data.fold$writedir <- fn.py.file(data.fold$basename)
      dir.create(data.fold$writedir, showWarnings = F, recursive = T)
      
      data.fold$col.out <- "IsClick"
      data.fold$cols.in <- cols.in
      
      tr.type <- fn.lr.tr.type(test.type)
      cat("\ntr.type", paste(tr.type, collapse=", "), "...\n")
      
      data.fold$tr.idx <- which(data.search.type %in% unique(tr.type))
      cat("\ntr.idx", length(data.fold$tr.idx), "...\n")
      
      data.fold$test.idx <- which(data.search.type %in% unique(test.type))
      cat("\ntest.idx", length(data.fold$test.idx), "...\n")
      data.fold$test.pred.file <- fn.file.data.fold(data.fold, "eval.pred")
      
      data.all.cur[, IsTestRow:=0]
      data.all.cur[data.fold$test.idx, IsTestRow:=1]
      all.ix <- sort(unique(c(data.fold$tr.idx, data.fold$test.idx)))
      cat("\nall.ix", length(all.ix), "...\n")
      
      cat("\nSaving", test.type, jx, "csv...\n")
      data.fold$all.file <- fn.file.data.fold(data.fold, "all.csv")
      fn.write.csv.chunk(
        data=data.all.cur, subset=all.ix,
        file=data.fold$all.file, row.names = F, compress = F
      )
      data.all.cur[, IsTestRow:=0]
      
      cat("\nSaving", test.type, "data.fold...\n")
      fn.save.data.fold(data.fold)
    }
  }
  NULL
}
fn.kill.wk()

#############################################################
# train phase
#############################################################
train.grid = expand.grid(
  test.type=c("tr", "val", "test"),
  jx=1:2,
  stringsAsFactors=F
)

fn.register.wk(nrow(train.grid))
data.ftrl.04.pred.tmp <- foreach(
  r=1:nrow(train.grid), .combine=rbind, .noexport=all.noexport) %dopar% {
  
  test.type <- train.grid$test.type[r]
  jx <- train.grid$jx[r]
  
  fn.init.fold.worker(paste0("ftrl_04_", jx), test.type)
  # fn.clean.worker()  
  
  epochs <- 1
  system(paste(
    "cd ../avito-context-click-py && pypy -u train_ftrl.py",
    "-train_file", data.fold$all.file,
    # "-train_model_file {TRAIN_FILE}.pklz",
    "-test_pred_file", data.fold$test.pred.file,
    "-test_pred_extra_cols ID -test_pred_col Pred",
    "-col_out IsClick",
    "-col_in_cat", 
     "    AdCatID AdHistCTRBin AdID AdParams AdPriceBin AdTitleSZBin ",
     "    Position ",
     "    SearchAdCount SearchAdT1Count SearchAdT2Count SearchAdT3Count ",
     "     SearchCatID SearchLocID",
     "     SearchParamsSZBin SearchQuerySZBin SearchRussian ",
     "    UserAgentFamilyID UserAgentID UserAgentOSID UserDeviceID ",
     "     UserID UserIPID UserPrevPhoneRequest ",
     "     UserPrevPrevPrevQryDateBin UserPrevPrevQryDateBin ",
     "     UserPrevQryDateBin UserPrevVisitReq UserPrevVisitReqUni ",
    "-train_is_test_col IsTestRow",
    "-bits 27 -alpha 0.07 -beta 1.0 -l1 0.01 -l2 1. -dropout 0",
    "-two_way 'Ad Us' 'Us Search' 'Ad Search' 'Ad Pos' 'Us Pos' 'Pos Search'",
    "-seed 7 -epochs", epochs,
    # "-load_model",
    " >> ", paste0("../data/log/", data.fold$logname, ".log"), " 2>&1"
  ))
  
  data.fold$test.pred <- fread(paste(data.fold$test.pred.file, epochs, sep="."))
  fn.print.err(data.fold$test.pred)

  fn.clean.worker()
  
  data.fold$test.pred
}
fn.kill.wk()

data.ftrl.04.pred.tmp <- data.ftrl.04.pred.tmp[order(ID)]
Store(data.ftrl.04.pred.tmp)

data.ftrl.04.pred <- data.ftrl.04.pred.tmp[
  ,list(
    Pred=sum(Pred)/.N
    ), by="ID"
]

fn.print.err(data.ftrl.04.pred)
#      Size     Loss
# 1 7888752  0.04148 - tr
# 1 8512834  0.04335 - val
# 1 16401586 0.04245 - all


Store(data.ftrl.04.pred)
# 
# # fn.write.submission(data.ftrl.04.pred, "data.ftrl.04.pred")

