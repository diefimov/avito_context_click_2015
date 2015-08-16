options(scipen=999)

fn.stats <- function(data.all, flist.col, target.col) {
  setnames(data.all, target.col, "target")
  data.stats <- c()
  for (f in flist.col) {
    setnames(data.all, f, "feature")
    stats.row <- c(length(unique(data.all[,feature])),
                   length(unique(data.all[!is.na(target),feature])),
                   length(unique(data.all[is.na(target),feature])),
                   max(data.all[,feature]),
                   max(data.all[!is.na(target),feature]),
                   max(data.all[is.na(target),feature]),
                   min(data.all[,feature]),
                   min(data.all[!is.na(target),feature]),
                   min(data.all[is.na(target),feature]),
                   mean(data.all[,feature]),
                   mean(data.all[!is.na(target),feature]),
                   mean(data.all[is.na(target),feature]),
                   median(data.all[,feature]),
                   median(data.all[!is.na(target),feature]),
                   median(data.all[is.na(target),feature]),
                   sd(data.all[,feature]),
                   sd(data.all[!is.na(target),feature]),
                   sd(data.all[is.na(target),feature]))
    data.stats <- rbind(data.stats, stats.row)
    setnames(data.all, "feature", f)
  }
  colnames(data.stats) <- c("unique","unique_tr","unique_test",
                            "max","max_tr","max_test",
                            "min","min_tr","min_test",
                            "mean","mean_tr","mean_test",
                            "median", "median_tr", "median_test",
                            "sd", "sd_tr", "sd_test")
  data.stats <- as.data.table(data.stats)
  data.stats[, feature := flist.col]
  setnames(data.all, "target", target.col)
  return (data.stats)
}

fn.multilogloss <- function(data.actual, data.predicted) {
  actual <- as.matrix(data.actual)
  predicted <- as.matrix(data.predicted)
  probs <- rowSums(actual*predicted)
  probs[which(probs>0.999999)] <- 0.999999
  probs[which(probs<0.000001)] <- 0.000001  
  return(-(1/nrow(actual))*sum(log(probs)))
}

fn.logloss <- function(actual, predicted, pred.min=0.000001, pred.max=0.999999) {
  predicted[which(predicted > pred.max)] <- pred.max
  predicted[which(predicted < pred.min)] <- pred.min
  error <- sum(-actual*log(predicted) - (1-actual)*log(1-predicted))/length(actual)
  return (error)
}

fn.mcrmse <- function(actual, predicted) {
  if (is.vector(predicted) & is.vector(actual)) {
    ix <- which(!is.na(actual))
    nsamples <- length(ix)
    return (sqrt(sum((actual[ix] - predicted[ix])^2)/nsamples))
  }
  if (ncol(actual) != ncol(predicted)) return (NULL)
  if (nrow(actual) != nrow(predicted)) return (NULL)
  ix <- which(!is.na(actual[,1]))
  nsamples <- length(ix)
  error <- 0
  #cat("Errors by targets:")
  errors <- c()
  for (i in 1:ncol(actual)) {
    error.col <- sqrt(sum((actual[ix,i] - predicted[ix,i])^2)/nsamples)
    errors <- c(errors, error.col)
    error <- error + error.col
    #cat(colnames(actual)[i],":",error.col,";")
  }
  #cat("\n")
  errors <- c(errors, error/ncol(actual))
  return (errors)
}

fn.memory.usage <- function() {
  return (sum(sort( sapply(ls(globalenv()),function(x){object.size(get(x))}))))
}

fn.write.batches.csv <- function(data, train.file, col.names, sep, nchunks = 4, continue.chunks=FALSE) {
  options(scipen=999)
  if (nchunks == 1) {
    write.table(
      data,
      file=train.file,
      row.names = F, quote = F, na = "", sep = sep,
      append = FALSE, col.names = col.names
    )
  } else {
    nr <- nrow(data)
    ix <- seq(1, nr, round(nr/nchunks))
    if (ix[length(ix)] != nr) {
      ix <- c(ix, nr+1)
    } else {
      ix[length(ix)] <- nr+1
    }
    gc()
    for (i in 1:(length(ix)-1)) {
      cat("Processing chunk", i, "...\n")
      if (i == 1 & !continue.chunks) {
        write.table(
          data[ix[i]:(ix[i+1]-1),],
          file=train.file,
          row.names = F, quote = F, na = "", sep = sep,
          append = FALSE, col.names = col.names
        )
      } else {
        write.table(
          data[ix[i]:(ix[i+1]-1),],
          file=train.file,
          row.names = F, quote = F, na = "", sep = sep,
          append = TRUE, col.names = FALSE
        )
      } 
      invisible(gc())
    }
  }
}

fn.optim <- function(y, x) {
  
  x <- as.matrix(x)
  pars0 <- rep(0.0, ncol(x))
  
  #error to minimize
  fn.loss <- function(pars) {
    y.pred <- 1 / (1 + exp(-as.numeric(x %*% pars)))
    y.pred <- pmax(y.pred, 10^(-6))
    y.pred <- pmin(y.pred, 1-10^(-6))
    sum(-y*log(y.pred) - (1-y)*log(1-y.pred))/length(y)
  } 
  
  cat ("Initial loss:", fn.loss(pars0), "\n")
  opt.result <- optim(pars0, 
                      fn.loss, 
                      #method = "Brent",
                      #method = "L-BFGS-B",
                      #lower = 0.0,
                      #upper = 10.0,
                      control = list(trace = T,maxit=5000))
  return (opt.result$par)
}
