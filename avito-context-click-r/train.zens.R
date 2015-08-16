##############################################################
## csvs
##############################################################


data.sub.dtry.0409xx <- fread(fn.submission.file(
  "dtry.xgb9__0.0409xx"))[order(ID)]

data.l1.xgb.03.pred <- data.l1.xgb.03.pred[ID > 0]

data.sub.ens.0404x <- merge(data.l1.xgb.03.pred,
                            data.sub.dtry.0409xx,
                            suffixes=c(".l",".d"),
                            by="ID")
setkey(data.sub.ens.0404x, ID)
data.sub.ens.0404x[, IsClick := IsClick.l^0.6 * IsClick.d^0.4 ]

data.l2.xgb.02.pred.val <- data.l2.xgb.02.pred[ID < 0]
data.l2.xgb.02.pred <- data.l2.xgb.02.pred.calib[ID > 0]
setkey(data.l2.xgb.02.pred, ID) # 0.04043


data.l1.xgb.05.pred <- data.l1.xgb.05.pred[ID > 0]
setkey(data.l1.xgb.05.pred, ID) #  0.04076



data.sub.ens <- data.sub.ens.0404x[, list(ID)]

data.sub.ens[, Pred.l2 :=  data.l2.xgb.02.pred$Pred]
data.sub.ens[, Pred.Ens1 := 
               data.sub.ens.0404x$IsClick^0.4
             * Pred.l2^0.6]
data.sub.ens[, Pred.Ens2 := 
               Pred.Ens1^0.9
             * data.l1.xgb.05.pred$Pred^0.1]

data.sub.ens[, Pred := Pred.Ens2]
data.sub.ens[, Pred := Pred.Ens2*1.1]

cat('Test avg:', mean(data.sub.ens$Pred), "\n") 
# Test avg: 0.007941962 

fn.write.submission(data.sub.ens, "data.sub.ens", mean.adj=T)



