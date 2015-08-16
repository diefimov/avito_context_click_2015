#############################################################
# train model
#############################################################

fn.register.wk(1, seed=5471887) # 5471887
data.xgb.03.pred.tmp <- foreach(
  test.type=c("test"), .combine=rbind, .noexport=all.noexport) %dopar% {
  
  fn.init.new.fold.worker("xgb_03", paste0(test.type))
  # fn.clean.worker()
  

  tr.type <- fn.lr.tr.type(test.type)
  # tr.type <- fn.tree.tr.type(test.type)
  
  data.fold$tr.idx <- which(data.all.tree$SearchType %in% tr.type)
  data.fold$test.idx <- which(!data.all.tree$SearchType %in% tr.type)
  data.fold$val.idx <- data.fold$test.idx[!is.na(
    data.all.tree$IsClick[data.fold$test.idx])]
  
  cols.extra <- c("ID", "SearchID", "SearchType", "IsClick")
  cols.in <- sort(setdiff(colnames(data.all.tree), 
                          c(cols.extra)))
  
  cat("\n\nTr size:", length(data.fold$tr.idx), 
      ", Val size:", length(data.fold$val.idx),
      ", Test size:", length(data.fold$test.idx),
      "...\n")
  
  data.tr <- fn.xgb.matrix(
    data=data.all.tree, subset=data.fold$tr.idx, col.in=cols.in)
  
  if (length(data.fold$val.idx) > 0) {
    data.val <- fn.xgb.matrix(
      data=data.all.tree, subset=data.fold$val.idx, col.in=cols.in)
    data.watch = list(val=data.val)
  } else {
    data.watch = list(tr=data.tr)
  }

  
  data.test <- fn.xgb.matrix(
    data=data.all.tree, subset=data.fold$test.idx, col.in=cols.in)
  
  data.fold$test.pred <- data.table(
    ID = data.all.tree$ID[data.fold$test.idx],
    Pred = 0.0,
    n = 0
  )
  
  avg.ix <- which(data.fold$test.pred$ID > 0)
  if (length(avg.ix) == 0) {
    avg.ix <- 1:nrow(data.fold$test.pred)
  }
  # print(length(avg.ix))
  
  fn.soar.unload(data.all.tree)

  data.fold$params = list(
    objective = "binary:logistic",
    eval_metric = "logloss",
    nthread = 12,
    eta = 0.2,
    max_depth = 10,
    gamma = 0.8, 
    colsample_bytree = 0.7, 
    colsample_bylevel = 0.8
  )
  
  data.fold$nrounds <- 75
  
  cat("\nParams:\n")
  print(data.fold$params)
  
  n.models <- 20
  for (ix in 1:n.models) {
    
    cat("\n\nTraining ", ix, "...\n")
    
    set.seed(ix + 89475560)
    
    suppressMessages(library("xgboost"))
    model =  xgb.train(
      data = data.tr,
      watchlist=data.watch,
      params = data.fold$params,
      nrounds = data.fold$nrounds,
      verbose = 1)
    
    
    ntreelimit <- data.fold$nrounds
    try.pred <- T
    
    while (try.pred) {
      pred.cur <- predict(model, data.test, ntreelimit=ntreelimit)
      pred.cur.avg <- mean(pred.cur[avg.ix])
      
      cat("\nCurrent prediction avg of", length(avg.ix),
          "instances:", pred.cur.avg, "\n")
      if (test.type == "val" || 
          (pred.cur.avg >= 0.008 && pred.cur.avg <= 0.012)) {
        
        try.pred <- F
        # data.fold$test.pred[ , Pred := (Pred*n + pred.cur)/(n+1)]
        # data.fold$test.pred[,  n := n+1]
        
        data.fold$test.pred[ , Pred := ((Pred^n)*pred.cur)^(1/(n+1))]
        data.fold$test.pred[,  n := n+1]
        
        fn.save.data.fold(data.fold)
        cat("\nPrediction with", ntreelimit ,"trees included\n")
      } else {
        cat("\nPrediction with", ntreelimit ,"trees discarded\n")
        ntreelimit <- ntreelimit - 5
        try.pred <- ntreelimit >= 60
      }
    }

    cat("\nPrediction status using", data.fold$test.pred$n[1], "iteration(s) :\n")
    fn.print.err(data.fold$test.pred)
    
    set.seed(Sys.time())
    rm(pred.cur, pred.cur.avg)
    invisible(gc())
    
  }
  
#   data.fold$importance <- xgb.importance(
#     feature_names=cols.in, model=model)
#   
#   cat("\n\nFeature importance:\n")
#   print(data.fold$importance)
  
  fn.clean.worker()
  
  data.fold$test.pred
  
}
fn.kill.wk()

data.xgb.03.pred.tmp <- data.xgb.03.pred.tmp[
  order(ID),list(Pred=mean(Pred)), by="ID"]
Store(data.xgb.03.pred.tmp)

data.xgb.03.pred <- copy(data.xgb.03.pred.tmp)

#############################################################
# save data
#############################################################

fn.print.err(data.xgb.03.pred)
#      Size    Loss
# 1 8512834 0.04256

Store(data.xgb.03.pred) # 0.04086

cat('Test avg:', mean(data.xgb.03.pred[ID > 0]$Pred), "\n") 
# Test avg: 0.008966203 

# fn.write.submission(data.xgb.03.pred, "data.xgb.03.pred")
