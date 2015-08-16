options(warn=-1)
suppressMessages(library("data.table"))
suppressMessages(library("compiler"))

enableJIT(3) 
setCompilerOptions(suppressUndefined = T)
options(stringsAsFactors = FALSE)
options(max.print = 500)
options(scipen=999)

path.wd <- getwd()
Sys.setenv("R_LOCAL_CACHE"="../data/output-r/.R_Cache")

`%ni%` <- Negate(`%in%`)

suppressMessages(library("ffbase"))
suppressMessages(library("SOAR"))
suppressMessages(library("SparseM"))
suppressMessages(library("Matrix"))
suppressMessages(library("matrixStats"))

suppressMessages(library("Rcpp"))
suppressMessages(library("xgboost"))

suppressMessages(library(infotheo))
suppressMessages(library(tm))

sourceCpp("_fn.base.cpp")

all.noexport <- c(Objects(), "fn_opt_gm", "fn_opt_am")

search.info.file <- "SearchInfo.tsv"
train.file <- "trainSearchStream.tsv"
test.file <- "testSearchStream.tsv"
user.info.file <- "UserInfo.tsv"
category.file <- "Category.tsv"
phone.request.stream.file <- "PhoneRequestsStream.tsv"
visit.stream.file <- "VisitsStream.tsv"
ads.info.file <- "AdsInfo.tsv"

#############################################################
# tic toc
#############################################################
tic <- function(gcFirst = TRUE, type=c("elapsed", "user.self", "sys.self")) {
  type <- match.arg(type)
  assign(".type", type, envir=baseenv())
  if(gcFirst) gc(FALSE)
  tic <- proc.time()[type]         
  assign(".tic", tic, envir=baseenv())
  invisible(tic)
}

toc <- function() {
  type <- get(".type", envir=baseenv())
  toc <- proc.time()[type]
  tic <- get(".tic", envir=baseenv())
  print(toc - tic)
  invisible(toc)
}

##############################################################
## Registers parallel workers
##############################################################
fn.register.wk <- function(n.proc = NULL, seed=5478557) {
  if (file.exists(fn.in.file("cluster.csv"))) {
    cluster.conf <- read.csv(fn.in.file("cluster.csv"), 
                             stringsAsFactors = F,
                             comment.char = "#")
    n.proc <- NULL
    for (i in 1:nrow(cluster.conf)) {
      n.proc <- c(n.proc, 
                  rep(cluster.conf$host[i], 
                      cluster.conf$cores[i]))
    }
  }
  if (is.null(n.proc)) {
    n.proc = as.integer(Sys.getenv("NUMBER_OF_PROCESSORS"))
    if (is.na(n.proc)) {
      suppressMessages(library(parallel))
      n.proc <-detectCores()
    }
  }
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (!exists(".pworkers", envir=baseenv()) || length(workers$.pworkers) == 0) {
    
    suppressMessages(library(doSNOW))
    suppressMessages(library(foreach))
    workers<-suppressMessages(makeSOCKcluster(n.proc))
    suppressMessages(registerDoSNOW(workers))
    suppressMessages(clusterSetupRNG(workers, seed=seed))
    assign(".pworkers", workers, envir=baseenv())
    
    tic()
    cat("Started", n.proc, "worker(s): ", 
        format(Sys.time(), format = "%Y-%m-%d %H:%M:%S"), "\n")
  }
  invisible(workers);
}

##############################################################
## Kill parallel workers
##############################################################
fn.kill.wk <- function() {
  suppressMessages(library("doSNOW"))
  suppressMessages(library("foreach"))
  workers <- mget(".pworkers", envir=baseenv(), ifnotfound=list(NULL));
  if (exists(".pworkers", envir=baseenv()) && length(workers$.pworkers) != 0) {
    suppressMessages(stopCluster(workers$.pworkers));
    assign(".pworkers", NULL, envir=baseenv());
    cat("Workers finish time: ", format(Sys.time(), 
                                        format = "%Y-%m-%d %H:%M:%S"), "\n")
    toc()
  }
  invisible(workers);
}

##############################################################
## init worker setting work dir and doing path redirect
##############################################################
fn.init.worker <- function(log = NULL, add.date = F) {
  
  source("_fn.base.R")
  setwd(path.wd)
  
  if (!is.null(log)) {
    date.str <- format(Sys.time(), format = "%Y-%m-%d_%H-%M-%S")
    
    if (is.list(log) && "logname" %in% names(log)) {
      log <- log[["logname"]]
    }
  
    
    if (add.date) {
      output.file <- fn.log.file(paste(log, "___",date.str,
                                       ".log", sep=""))
    } else {
      output.file <- fn.log.file(paste(log,".log", sep=""))
    }
    dir.create(dirname(output.file), showWarnings = F, recursive = T)
    output.file <- file(output.file, open = "wt")
    sink(output.file)
    sink(output.file, type = "message")
    
    cat("Start:", date.str, "\n")
  }
  
  tic()
}

##############################################################
## clean worker resources
##############################################################
fn.clean.worker <- function() {
  gc()
  
  try(toc(), silent=T)
  suppressWarnings(sink())
  suppressWarnings(sink(type = "message"))
  
  invisible(NULL)
}

##############################################################
## wait clean
##############################################################
fn.gc.wait <- function() {
  invisible(gc())
  Sys.sleep(1)
  invisible(gc())
}

#############################################################
# log file path
#############################################################
fn.base.dir <- function(extra) {
  paste0(path.wd, "/../data/", extra)
}

#############################################################
# log file path
#############################################################
fn.log.file <- function(name) {
  fn.base.dir(paste0("log/", name))
}

#############################################################
# input file path
#############################################################
fn.in.file <- function(name) {
  fn.base.dir(paste0("input/", name))
}

#############################################################
# R output file path
#############################################################
fn.out.file <- function(name) {
  fn.base.dir(paste0("output-r/", name))
}

#############################################################
# python output file path
#############################################################
fn.py.file <- function(name) {
  fn.base.dir(paste0("output-py/", name))
}

#############################################################
# libfm output file path
#############################################################
fn.libffm.file <- function(name) {
  fn.base.dir(paste0("output-libffm/", name))
}

#############################################################
# dropbox file path
#############################################################
fn.dropbox.file <- function(name) {
  fn.base.dir(paste0("dropbox/", name))
}

#############################################################
# matlab file path
#############################################################
fn.ml.file <- function(name) {
  fn.base.dir(paste0("output-ml/", name))
}

#############################################################
# ranklib
#############################################################
fn.ranklib.file <- function(name) {
  fn.base.dir(paste0("output-ranklib/", name))
}

#############################################################
# rdata files
#############################################################
fn.rdata.file <- function(name) {
  fn.base.dir(paste0("rdata/", name))
}

#############################################################
# libsvm files
#############################################################
fn.libsvm.file <- function(name) {
  fn.base.dir(paste0("libsvm/", name))
}

#############################################################
# submission file path
#############################################################
fn.submission.file <- function(name, suffix=".csv") {
  fn.base.dir(paste0("submission/", name, suffix))
}

#############################################################
# data file path
#############################################################
fn.data.file <- function(name) {
  fn.out.file(name)
}

#############################################################
# save data file
#############################################################
fn.save.data <- function(dt.name, envir = parent.frame()) {
  save(list = dt.name, 
       file = fn.data.file(paste0(dt.name, ".RData")), envir = envir)
}

#############################################################
# load saved file
#############################################################
fn.load.data <- function(dt.name, envir = parent.frame()) {
  load(fn.data.file(paste0(dt.name, ".RData")), envir = envir)
}

#############################################################
# round predictions
#############################################################
fn.round.pred <- function(pred) {
  round(pred, digits=6)
}

#############################################################
# bounded log loss
#############################################################
fn.log.loss <- function(actual, pred) {
  suppressMessages(library("Metrics"))
  ix <- !is.na(actual)
  if (!any(ix)) {
    return (NA_real_)
  }
  pred <- pmin(1-1e-15, pmax(1e-15, pred[ix]))
  actual <- actual[ix]
  return (logLoss(actual, pred))
}

#############################################################
# error evaluation
#############################################################
fn.print.err <- function(data.pred, do.print = T, scale=1.0) { 
  
  data.pred <- merge(data.pred[, list(ID, Pred)], 
                     data.all.out.small[, list(ID, IsClick)],
                     by="ID")
  data.pred.ix <- !is.na(data.pred$IsClick)
  data.pred[, Pred := fn.round.pred(Pred)*scale]

  df <- data.frame(Size = nrow(data.pred))
  if (sum(data.pred.ix) == 0) {
    df[["Avg"]] = mean(data.pred$Pred)
  } else {
    df[["Size"]] = sum(data.pred.ix)
    df[["Loss"]] <- round(fn.log.loss(data.pred[data.pred.ix]$IsClick,
                                      data.pred[data.pred.ix]$Pred), 
                          digits=5)
  }
  
  if (do.print) {
    print(df)
  }
  
  invisible(df)
}
# debug(fn.print.err)


#############################################################
# gets train type
#############################################################
fn.lr.tr.type <- function(test.type) {
  tr.type <- c("hist")
  if (test.type %in% c("val", "test")) {
    tr.type <- c(tr.type, "tr")
  }
  if (test.type %in% "test") {
    tr.type <- c(tr.type, "val")
  }
  return(tr.type)
}

#############################################################
# gets train type
#############################################################
fn.tree.tr.type <- function(test.type) {
  if (test.type == "test") {
    return ("val")
  } else {
    return ("tr")
  }
}

#############################################################
# gets train type
#############################################################
fn.l2.tr.type <- function(test.type) {
  if (test.type == "tr") {
    return ("val")
  }
  if (test.type == "val") {
    return ("tr")
  }
  if (test.type == "test") {
    return (c("tr", "val"))
  }
}

#############################################################
# load libfm predictions
#############################################################
fn.load.libfm.pred <- function(data.fold, iters=1) {
  data.pred <- data.table(ID=data.fold$test.ids, Pred=0)
  for (it in 1:iters) {
    data.pred[, Pred := Pred + 
                fread(paste(data.fold$test.pred.file, it, sep="."), 
                      header=F)$V1/iters]
  }
  data.pred
}

#############################################################
# creates data.fold for models
#############################################################
fn.create.data.fold <- function(model.name, model.type) {
  data.fold <- list()
  data.fold$basename <- model.name
  data.fold$model.type <- model.type
  data.fold$name <- paste0(data.fold$basename, "_", model.type)
  data.fold$logname <- paste(data.fold$basename, data.fold$name, sep="/")
  data.fold$fname <- fn.out.file(paste0(data.fold$basename, "/", 
                                        data.fold$name, ".RData"))
  data.fold
}


fn.load.data.fold <- function(model.name, model.type, envir=parent.frame()) {
  load(fn.out.file(paste0(model.name, "/", model.name,
                          "_", model.type, ".RData")),
       envir=envir)
}

fn.file.data.fold <- function(data.fold, suffix) {
  paste0(data.fold$writedir, "/data.", data.fold$model.type, ".", suffix)
}

fn.init.fold.worker <- function(model.name, model.type, envir=parent.frame(),
                                no.log=F) {
  source("_fn.base.R")
  fn.load.data.fold(model.name = model.name, model.type = model.type,
                    envir = envir)
  assign('data.fold', data.fold, envir=envir)
  if (no.log) {
    fn.init.worker()
  } else {
    fn.init.worker(data.fold)
  }
  invisible(data.fold)
}

fn.init.new.fold.worker <- function(model.name, model.type, 
                                    envir=parent.frame(), ...) {
  source("_fn.base.R")
  data.fold <- fn.create.data.fold(model.name, model.type)
  assign('data.fold', data.fold, envir=envir)
  fn.init.worker(data.fold, ...)
  invisible(data.fold)
}

#############################################################
# saves data from models for later inspection
#############################################################
fn.save.data.fold <- function(data.fold) {
  dir.create(dirname(data.fold$fname), showWarnings = F, recursive = T)
  save(data.fold, file=data.fold$fname)
}


#############################################################
# copies data from one model to another
#############################################################
fn.copy.data.fold <- function(from, to) {
  
  for (test.type in c("tr", "val", "test")) {
    fn.load.data.fold(from, test.type)
    for (nam in names(data.fold)) {
      if (is.character(data.fold[[nam]])) {
        data.fold[[nam]] <- gsub(from, to, data.fold[[nam]], fixed=T)
      }
    }
    fn.save.data.fold(data.fold)
  }
  
}

##################################################
# expand factors
##################################################
fn.expand.factors <- function(data.df) {
  data.frame(model.matrix( ~ . - 1, data=data.df))
}

##############################################################
## get with default value
##############################################################
fn.get <- function(var.name, def.val = NULL) {
  if ( exists(var.name)) {
    return(get(var.name))
  } else {
    return(def.val)
  }
}

#############################################################
# print rf importance
#############################################################
fn.rf.print.imp <- function(rf) {
  imp <- try(data.frame(
    importance=rf$importance[order(-rf$importance[,1]),]), silent = T)
  imp$importance <- imp$importance/sum(imp$importance)*100
  print(imp)
  imp
}

#############################################################
# write submission
#############################################################
fn.write.submission <- function(data.pred, file.name, mean.adj=F) { 
  tic()
  data.sub <- merge(
    data.pred[ID > 0, list(ID, Pred)],
    data.all.out.small[, list(ID, Position)],
    by="ID")
  data.pos.avg <- data.sub[ ,list(N=.N, Pred=mean(Pred)),by="Position"]
  if (mean.adj) {
    data.sub <- fn.mean.adj(data.sub)
    data.pos.avg <- merge(
      data.pos.avg, data.sub[ ,list(Pred=mean(Pred)),by="Position"],
      by="Position", suffixes=c("Raw", "Adj")
    )
    data.pos.avg[, Mult := round(PredAdj/PredRaw, digits=3)]
  }
  print(data.pos.avg)
  data.sub[, Pred := fn.round.pred(Pred)]
  setnames(data.sub, "Pred","IsClick")
  data.sub[, IsClick := pmax(0.0, pmin(1.0, IsClick))]
  write.csv(data.sub[, list(ID, IsClick)],
            file = xzfile(paste0(fn.submission.file(file.name), ".7z")), 
            row.names = F, quote = F)
  toc()
  invisible(data.sub)
}

#############################################################
# adjust mean of predictions
#############################################################
fn.mean.adj <- function(data.pred) {
  cols.add <- setdiff(c("Position"), colnames(data.pred))
  if (length(cols.add) > 0) {
    data.pred <- merge(
      data.pred,
      data.all.search.small[, c("ID", cols.add), with=F],
      by="ID")
  }
  data.search.avg <- data.table(
    Position = c(1,7),
    IsClickAvg = c(0.011927465*0.975, 0.006011063*0.93)
  )
  data.pred.avg <- data.pred[,list(PredAvg=mean(Pred)),
                             c("Position")]
  data.pred.avg <- merge(
    data.search.avg,
    data.pred.avg,
    by=c("Position"))
    
  data.pred <- merge(
      data.pred,
      data.pred.avg,
      by=c("Position"))
  
  data.pred[, Pred:=Pred/PredAvg*IsClickAvg]
  for (col.nam in cols.add) {
    data.pred[, col.nam := NULL, with=F]
  }
  data.pred[, IsClickAvg := NULL]
  data.pred[, PredAvg := NULL]
  data.pred[order(ID)]
}

##############################################################
## find mode
##############################################################
fn.mode <- function(x) {
  ux <- unique(x)
  ux[which.max(tabulate(match(x, ux)))]
}

##############################################################
## parse dates
##############################################################
fn.parse.date <- function(date.all, ...) {
  date.all.unique <- unique(date.all)
  date.parsed <- as.POSIXct(date.all.unique, ...)
  date.map <- data.table(
    val=date.all.unique, date=date.parsed,
    key="val")
  date.map[J(date.all)]$date
}

##############################################################
## calibrate probabilities
##############################################################
fn.calibrate.prob <- function(data.pred) {
  data.pred <- merge(data.pred[, list(ID, Pred)], 
                     data.all.search.small[, list(ID, IsClick, Position)],
                     by="ID")
  ix.has.click <- !is.na(data.pred$IsClick) 
  pos.all <- sort(unique(data.pred$Position))
  suppressMessages(library("rPython"))
  python.load("../avito-context-click-py/util_rpython.py")
  
  data.pred.calib.all <- NULL
  # for (pos.cur in pos.all) {
    # ix.pos <- data.pred$Position == pos.cur
    ix.pos <- data.pred$Position %in% pos.all
    ix.val <- ix.has.click & ix.pos
    ix.test <- !ix.has.click & ix.pos
    data.pred.calib <- python.call('calibrate_probs',
                                   y_val=data.pred[ix.val]$IsClick,
                                   prob_val=data.pred[ix.val]$Pred,
                                   prob_test=data.pred[ix.test]$Pred)
    data.pred.calib.all <- rbind(
      data.pred.calib.all, 
      data.table(
        ID = c(data.pred[ix.val]$ID, data.pred[ix.test]$ID),
        Pred = c(data.pred.calib$val, data.pred.calib$test)
      ))
  # }
  # fn.mean.adj(data.pred.calib.all[order(ID)])
  data.pred.calib.all[order(ID)]
}

##############################################################
## calibrate probabilities - worker version
##############################################################
fn.calibrate.prob.wk <- function(data.pred) {
  fn.register.wk(1)
  data.pred.calib <- foreach(
    tmp=1, .combine=rbind) %dopar% {
      fn.init.worker()
      fn.calibrate.prob(data.pred)
    }
  fn.kill.wk()
  data.pred.calib
}

##############################################################
## write csv using chunks
##############################################################
fn.write.csv.chunk <- function (data, subset=1:nrow(data), 
                                file, 
                                row.names = F, 
                                na = "", 
                                compress=T,
                                ...)  {

	if (is.logical(subset)) {
		subset <- which(subset)
	}
	
  append <- F
	for (ix.ch in chunk(from=1,to=length(subset),by=5000000, maxindex=length(subset))) {
		ix = subset[ix.ch[1]:ix.ch[2]]
		write.csv(
        data[ix,],
        file=file,
        append=append,
        row.names=row.names,
        na=na,
        ...
      )
		append=T
	}
  if (compress) {
    system(paste("pigz -f", file))
  }
}

##############################################################
## unloads SOAR objects from emory
##############################################################
fn.soar.unload <- function (..., list = character(0), lib = Sys.getenv("R_LOCAL_CACHE", 
    unset = ".R_Cache"), lib.loc = Sys.getenv("R_LOCAL_LIB_LOC", 
    unset = ".")) 
{
  .enc  <- local({
  
    bad <- c(" ", "<", ">", ":", "\"", "/", "\\", "|", "?", "*")
    rpl <- paste("@", 0:9, sep = "")
  
    regex <- paste("(", paste(LETTERS, collapse = "|"), ")", sep = "")
  
    function (x) {
      x <- gsub("@", "\t", x, fixed = TRUE)
      for (i in seq(along = bad))
        x <- gsub(bad[i], rpl[i], x, fixed = TRUE)
      x <- gsub(regex, "@\\1", x)
      x <- gsub("\t", "@@", x, fixed = TRUE)
      paste(x, "@.RData", sep = "")
    }
  })
  
  .dec  <- local({
  
    bad <- c(" ", "<", ">", ":", "\"", "/", "\\", "|", "?", "*")
    rpl <- paste("@", 0:9, sep = "")
  
    regex <- paste("@(", paste(LETTERS, collapse = "|"), ")", sep = "")
  
    function (x) {
      x <- gsub("@@", "\t", x, fixed = TRUE)
      x <- sub("@\\.RData$", "", x)
      x <- sub("\\.RData$", "", x)
      x <- gsub(regex, "\\1", x)
      for (i in seq(along = bad))
        x <- gsub(rpl[i], bad[i], x, fixed = TRUE)
      gsub("\t", "@", x, fixed = TRUE)
    }
  })
  
  .mostFiles <- function(path)
    setdiff(dir(path, all.files = TRUE), c(".", ".."))
  
  .pathAttributes <- function () {
    s <- search()
    paths <- lapply(1:length(s),
                    function(i) attr(as.environment(i), "path"))
    paths[[length(s)]] <- system.file()
    m <- sapply(paths, is.null)
    paths[m] <- s[m]
    unlist(paths)
  }
  
  .attach <- function(directory, pos = 2,
                      warn = !file.exists(directory),
                      readonly) {
    env <- attach(NULL, pos, basename(directory))
    attr(env, "path") <- directory
    attr(env, "readonly") <- readonly
    if (file.exists(directory)) {
      fils <- .mostFiles(directory)
      objs <- .dec(fils)
      fils <- file.path(directory, fils)
      for(i in seq(along = objs))
        eval(substitute(delayedAssign(OBJECT, {
          load(file = FILE)
          get(OBJECT)
        }), list(OBJECT = objs[i], FILE = fils[i])),
             envir = env)
    } else if (warn)
      warning(paste(directory,
                    "does not currently exist. ",
                    "A call to 'Store' will create it."),
              call. = FALSE)
  }
  
  .makeClone <- function(Name, Which) {
    dsn <- deparse(substitute(Name))
    dsw <- deparse(substitute(Which))
    f <- function(...) {}
    body(f) <- substitute({
      Call <- match.call()
      Call[[1]] <- quote(SOAR::NAME)
      if(is.null(Call[["lib"]]))
        Call[["lib"]] <- Sys.getenv(LIB, unset = WHICH)
      if(is.null(Call[["lib.loc"]]))
        Call[["lib.loc"]] <- Sys.getenv(LIB_LOC, unset = path.expand("~"))
      eval.parent(Call)
    }, list(NAME = as.name(dsn),
            WHICH = paste(".R", dsw, sep = "_"),
            LIB = paste("R_CENTRAL", toupper(dsw), sep = "_"),
            LIB_LOC = "R_CENTRAL_LIB_LOC"))
    f
  }
  
    if (class((.x <- substitute(lib))) == "name") 
        lib <- deparse(.x)
    else lib <- lib
    if (!(file.exists(lib.loc) && file.info(lib.loc)$isdir)) 
        stop(lib.loc, " is not an existing directory.", call. = FALSE, 
            domain = NA)
    path <- file.path(lib.loc, lib)
    if (file.exists(path) && !file.info(path)$isdir) 
        stop(path, " exists but is not a directory!", call. = FALSE, 
            domain = NA)
    if (m <- match(path, .pathAttributes(), nomatch = FALSE)) {
        e <- as.environment(m[1])
        if (!is.null(n <- attr(e, "readonly")) && n) 
            stop(path, " is attached as read only!", call. = FALSE, 
                domain = NA)
    }
    nam <- list
    if (!is.null(m <- match.call(expand.dots = FALSE)$...)) 
        nam <- c(nam, sapply(m, function(x) switch(class(x), 
            name = deparse(x), call = {
                o <- eval(x, envir = parent.frame(n = 4))
                if (!is.character(o)) stop("non-character name!", 
                  call. = FALSE, domain = NA)
                o
            }, character = x, stop("garbled call to 'Store'", 
                call. = FALSE, domain = NA))))
    if (length(nam) == 0) 
        return()
    nam <- drop(sort(unique(nam)))
    for (n in nam) {
        no <- !exists(n, inherits = FALSE, envir = parent.frame())
        comm <- if (no) 
            substitute({
                assign(N, get(N))
                rm(list = N)
            }, list(N = n))
        eval.parent(comm)
    }
    pos <- if (any(m <- (.pathAttributes() == path))) {
        m <- which(m)[1]
        detach(pos = m)
        m
    }
    else 2
    .attach(path, pos = pos, warn = FALSE, readonly = FALSE)
    o <- intersect(eval.parent(quote(objects(all.names = TRUE))), 
                   nam)
    if (length(o) > 0) 
      eval.parent(substitute(remove(list = O), list(O = o)))
    
    invisible(gc())
}

##############################################################
## load ensemble data
##############################################################
fn.load.ens <- function(ens.cols, transf=identity,
                        print.err = T) {
  
  data.ens.pred <- NULL
  err.all <- NULL
  for (pred.nam in unique(ens.cols)) {
    pred.nam.orig <- pred.nam
    data.cur <- get(pred.nam)[,list(ID, Pred)]
    data.cur <- transf(data.cur)
    if (is.null(data.ens.pred)) {
      data.ens.pred <- data.table(data.cur)
    } else {
      data.ens.pred <- merge(data.ens.pred, data.cur, by="ID")
    }
    pred.nam <- gsub("^data\\.", "", pred.nam)
    pred.nam <- gsub("\\.pred$", "", pred.nam)
    if (print.err) {
      df.err <- fn.print.err(data.ens.pred, do.print = F)
      err.cur <- df.err$Loss
      names(err.cur) <- pred.nam
      err.all <- c(err.all, err.cur)
    }
    setnames(data.ens.pred, "Pred", pred.nam)
    fn.soar.unload(list=pred.nam.orig)
  }
  if (!is.null(err.all)) {
    print(err.all)
  }
  data.ens.pred[order(ID)]
}

##############################################################
## print correlations
##############################################################
fn.ens.cor <- function(data.ens.pred, first.col = NULL, do.print=T) {
  data.ens.cols <- setdiff(colnames(data.ens.pred), c("ID"))
  data.ens.cor <- data.frame(t(combn(length(data.ens.cols), 2)))
  colnames(data.ens.cor) <- c("col1", "col2")
  data.ens.cor$linear_cor <- NA_real_ 
  data.ens.cor$mean_diff <- NA_real_ 
  # data.ens.cor$mean_abs_diff <- NA_real_ 
  data.ens.cor$sd <- NA_real_ 
  data.ens.cor$col1 <- data.ens.cols[data.ens.cor$col1]
  data.ens.cor$col2 <- data.ens.cols[data.ens.cor$col2]
  if (!is.null(first.col)) {
    fn.sel.col <- function(cols) {
      as.logical(apply(sapply(first.col,  function(x) cols %like% x), 1, max))
    }
    data.ens.cor <- data.ens.cor[
      fn.sel.col(data.ens.cor$col1) | fn.sel.col(data.ens.cor$col2),]
  }
  for (ix in 1:nrow(data.ens.cor)) {
    p1 <- data.ens.pred[[data.ens.cor$col1[ix]]]
    p2 <- data.ens.pred[[data.ens.cor$col2[ix]]]
    pred.ix <- which((p1 != 0 | p2 != 0) & data.ens.pred$ID > 0)
    p1 <- p1[pred.ix]
    p2 <- p2[pred.ix]
    data.ens.cor$linear_cor[ix] <- cor(p1, p2)
    data.ens.cor$mean_diff[ix] <- mean(p1 - p2)
    # data.ens.cor$mean_abs_diff[ix] <- mean(abs(p1 - p2))
    data.ens.cor$sd[ix] <- sd(p1 - p2)
  }
  if (do.print) {
    print(data.ens.cor)
  }
  invisible(data.ens.cor)
}

##############################################################
## cross val folds
##############################################################
fn.cv.ens.folds <- function(seed = 34234) {
  
  ids <- data.all.search.small[SearchType=="val", list(ID, SearchID)]
  ids.search <- sort(unique(ids$SearchID))
  n <- length(ids.search)
  suppressMessages(library("cvTools"))
  
  if (!is.null(seed)) {
    set.seed(seed)
  }
  data.cv.folds <- cvFolds(n, K = 2, type = "random")
  if (!is.null(seed)) {
    set.seed(Sys.time())
  }
  
  data.cv.folds <- list(
    n = data.cv.folds$n,
    K = data.cv.folds$K,
    which = data.cv.folds$which[data.cv.folds$subsets][1:n],
    ids = ids,
    ids.search = ids.search
  )
  data.cv.folds
}

##############################################################
## cross val selection
##############################################################
fn.cv.which <- function(data.all, k, type, cv.data=data.cv.ens) {
  sel.ids <- cv.data$ids.search[!cv.data$which %in% k]
  sel.ids <- cv.data$ids[SearchID %in% sel.ids]$ID
  ix <- data.all$ID %in% sel.ids
  if (type == "tr") {
    return (which(ix))
  } else {
    return (which(!ix))
  }
}

#############################################################
# optmin logloss regression - arithmethic average
#############################################################
fn.opt.lr.pred <- function(pars, x) {
  if (is.list(pars) && "par" %in% names(pars)) {
   pars <- pars$par 
  }
  if (!is.matrix(x)) {
    x <- as.matrix(x)
  }
  bias <- 0
  if (ncol(x) != length(pars)) {
    bias <- pars[1]
    pars <- pars[-1]
  }
  (x %*% pars)[,1] + bias
}

#############################################################
# optmin logloss regression - geometric average
#############################################################
fn.opt.gm.pred <- function(pars, x) {

  if (is.list(pars) && "par" %in% names(pars)) {
    pars <- pars$par 
  }
  if (!is.matrix(x)) {
    x <- as.matrix(x)
  }
  bias <- 0
  if (ncol(x) != length(pars)) {
    bias <- pars[1]
    pars <- pars[-1]
  }
  rowProds(sapply(1:ncol(x), function (i) x[,i]^pars[i])) + bias
}

##############################################################
## optmizes log loss
##############################################################
fn.opt.ll.train <- function(x, y, pars=rep(1/ncol(x),ncol(x)), bias=NA, 
                            fn.opt.cfg) {
  if (!is.matrix(x)) {
    x <- as.matrix(x)
  }
  if (!is.na(bias)) {
    pars <- c(bias, pars)
  }
  
  if (is.null(fn.opt.cfg$fn.opt)) {
    fn.opt <- function(pars) fn.log.loss(y, fn.opt.cfg$fn.pred(pars, x))
  } else {
    fn.opt <- function(pars) fn.opt.cfg$fn.opt(x, y, pars)
  }
  
  res <- optim(pars, control = list(trace = T), fn.opt)
  if (!is.na(bias)) {
    names(res$par) <- c("bias", colnames(x))
    res$par[-1] <- res$par[-1]/sum(res$par[-1])
  } else {
    names(res$par) <- colnames(x)
    res$par <- res$par/sum(res$par)
  }
  
  
  res$fn.pred <- fn.opt.cfg$fn.pred
  
  res
}

  
##############################################################
## calculates prob counts
##############################################################
fn.build.prob.stats <- function(
  data.all,
  cols.stats,
  ids.tr,
  ids.test,
  use.slide
  ) {
  
  data.all.stats <- data.all[
    get('ID') %in% unique(c(ids.tr, ids.test)),
    c("ID", "SearchDate", "SearchID", "IsClick", cols.stats),
    with=F]
  setkeyv(data.all.stats, c("SearchDate", "SearchID"))
  data.all.stats[ID %in% ids.test, IsClick := NA_real_]
  
  if (use.slide) {
    data.all.stats <- data.all.stats[
      ,list(
        ID = ID,
        pos = cumsum(IsClick %in% c(1)),
        n = cumsum(!is.na(IsClick))
      ), by = cols.stats][ID %in% ids.test]
  } else {
    data.all.stats <- merge(
      data.all.stats[get('ID') %in% ids.test, c("ID", cols.stats), with=F],
      data.all.stats[
        ID %in% ids.tr,
        list(
          pos = sum(IsClick, na.rm=T),
          n = sum(!is.na(IsClick))
        ), by = cols.stats],
      by=cols.stats, all.x=T
    )
    data.all.stats[is.na(pos), pos := 0]
    data.all.stats[is.na(n), n := 0]
  }
  
  invisible(gc())
  for (col.nam in cols.stats) {
    data.all.stats[, col.nam := NULL, with=F]
  }
  setkey(data.all.stats, ID)
  data.all.stats
}

##############################################################
## calculates prob counts
##############################################################
fn.build.prob <- function(
  cols.prob.list, data.all=data.all.lr.small,
  k.opt=round(10^seq(1, 2.5, 0.5)),
  k.max=500
) {
  
  data.prob.all <- data.all.out.small[order(ID), list(ID, IsClick)]
  data.click.mean <- mean(data.prob.all$IsClick, na.rm=T)
  for (cols.stats in cols.prob.list) {
    col.target <- paste(c("Prob", cols.stats), collapse="")
    cat("\nCalculating", col.target, "from", 
        paste(cols.stats, collapse=" "), ": ")
    cat("tr, ")
    data.prob.stats.tr <- fn.build.prob.stats(
      data.all = data.all,
      cols.stats = cols.stats,
      ids.tr = data.all$ID[as.character(data.all$SearchType) %chin% "hist"],
      ids.test = data.all$ID[as.character(data.all$SearchType) %chin% "tr"],
      use.slide = F
    )
    invisible(gc())
    cat("val, ")
    data.prob.stats.val <- fn.build.prob.stats(
      data.all = data.all,
      cols.stats = cols.stats,
      ids.tr = data.all$ID[as.character(data.all$SearchType) %chin% c("hist", "tr")],
      ids.test = data.all$ID[as.character(data.all$SearchType) %chin% "val"],
      use.slide = F
    )
    invisible(gc())
    cat("test")
    data.prob.stats.test <- fn.build.prob.stats(
      data.all = data.all,
      cols.stats = cols.stats,
      ids.tr = data.all$ID[as.character(data.all$SearchType) %chin% c("hist", "tr", "val")],
      ids.test = data.all$ID[as.character(data.all$SearchType) %chin% "test"],
      use.slide = F
    )
    invisible(gc())
    cat(" - done. k fitting started...\n")
    data.prob.stats <- rbind(
      #data.prob.stats.hist,
      data.prob.stats.tr, 
      data.prob.stats.val,
      data.prob.stats.test)
    setkeyv(data.prob.stats, "ID")
    rm(data.prob.stats.tr, # data.prob.stats.hist, 
       data.prob.stats.val, data.prob.stats.test)
    invisible(gc())
    
    if (!all(data.prob.stats$ID == data.prob.all$ID)) {
      stop('IDs of instances do not match!')
    }
    
    # data.prob.stats[, w := fn.transf(w)/w]
    
    # calculate smoothed averages
    fn.opt.prob.cal <- function(pos, n, k, avg=data.click.mean)
        {round((pos + k*avg)/(n+max(c(0, min(c(k.max, k))))), digits=6) }
    fn.opt.prob.loss <- function(pars) {
      fn.log.loss(data.prob.all$IsClick, 
                  fn.opt.prob.cal(pos=data.prob.stats$pos,
                               n=data.prob.stats$n,
                               k=pars)) 
    }
    if (is.null(k.opt)) {
      best.k <- optim(10, control = list(trace = F), fn.opt.prob.loss)
    } else {
      k.loss <- sapply(k.opt, fn.opt.prob.loss)
      k.ix <- which.min(k.loss)
      best.k <- list(par=k.opt[k.ix], value=k.loss[k.ix])
    }
    cat(col.target, 'best k:', best.k$par, ', loss:', best.k$value, '\n')
    data.prob.stats[, Pred := fn.opt.prob.cal(pos=pos, n=n, k=best.k$par)]
    data.prob.all[, col.target := data.prob.stats$Pred, with=F]
    rm(data.prob.stats)
    invisible(gc())
  }
  data.prob.all[, IsClick := NULL]
  return (data.prob.all)
}

##############################################################
## calculates prob counts
##############################################################
fn.build.prob.stats.full <- function(
  data.all,
  cols.stats,
  sign=1
  ) {
  
  data.all.stats <- data.all[
    , c("ID", "SearchDate", "SearchID", "IsClick", cols.stats), with=F]
  data.all.stats[, SearchDate := sign*SearchDate]
  setkey(data.all.stats, SearchDate, SearchID)
  
  fn.prev.val <- function(x) c(0, x[-length(x)]) 
  data.all.stats <- data.all.stats[
    ,list(
      ID = ID,
      is_second = SearchID == fn.prev.val(SearchID),
      cur_pos = as.numeric(IsClick == 1 & !is.na(IsClick)),
      pos = cumsum(IsClick == 1 & !is.na(IsClick)),
      cur_n = as.numeric(!is.na(IsClick)),
      n = cumsum(!is.na(IsClick))
    ), by = cols.stats]
  
  data.all.stats[, pos := pos - cur_pos - is_second*fn.prev.val(cur_pos)]
  data.all.stats[, n := n - cur_n  - is_second*fn.prev.val(cur_n)]
  
  invisible(gc())

  cols.rm <- setdiff(colnames(data.all.stats), c("ID", "n", "pos"))
  for (col.nam in cols.rm) {
    data.all.stats[, col.nam := NULL, with=F]
  }
  invisible(gc())
  setkey(data.all.stats, ID)
  data.all.stats
}

##############################################################
## calculates prob counts
##############################################################
fn.build.prob.full <- function(
  cols.prob.list, data.all=data.all.lr.small,
  k.opt=c(30, 100),
  k.max=500
) {
  
  data.prob.all <- data.all.out.full[order(ID), list(ID, IsClick)]
  fn.soar.unload(data.all.out.full)
  invisible(gc())
  
  for (cols.stats in cols.prob.list) {
    col.target <- paste(c("Prob", cols.stats), collapse="")
    cat("\nCalculating", col.target, "from", 
        paste(cols.stats, collapse=" "), ": ")
    cat("hist, ")
    data.prob.stats <- fn.build.prob.stats.full(
      data.all = data.all,
      cols.stats = cols.stats
    )
    setkey(data.prob.stats, ID)
    invisible(gc())
    
    cat("tr, ")
    data.prob.stats.tr <- fn.build.prob.stats(
      data.all = data.all,
      cols.stats = cols.stats,
      ids.tr = data.all$ID[as.character(data.all$SearchType) %chin% "hist"],
      ids.test = data.all$ID[as.character(data.all$SearchType) %chin% "tr"],
      use.slide = F
    )
    setkey(data.prob.stats.tr, ID)
    tr.ix <- which(data.prob.stats$ID %in% data.prob.stats.tr$ID)
    if (!all(data.prob.stats$ID[tr.ix] == data.prob.stats.tr$ID)) {
      stop('Ids dont match')
    }
    data.prob.stats[tr.ix, n:= data.prob.stats.tr$n]
    data.prob.stats[tr.ix, pos:= data.prob.stats.tr$pos]
    rm(data.prob.stats.tr)
    invisible(gc())
    
    cat("val, ")
    data.prob.stats.val <- fn.build.prob.stats(
      data.all = data.all,
      cols.stats = cols.stats,
      ids.tr = data.all$ID[as.character(data.all$SearchType) %chin% c("hist", "tr")],
      ids.test = data.all$ID[as.character(data.all$SearchType) %chin% "val"],
      use.slide = F
    )
    setkey(data.prob.stats.val, ID)
    val.ix <- which(data.prob.stats$ID %in% data.prob.stats.val$ID)
    if (!all(data.prob.stats$ID[val.ix] == data.prob.stats.val$ID)) {
      stop('Ids dont match')
    }
    data.prob.stats[val.ix, n:= data.prob.stats.val$n]
    data.prob.stats[val.ix, pos:= data.prob.stats.val$pos]
    rm(data.prob.stats.val)
    invisible(gc())
    
    cat("test")
    data.prob.stats.test <- fn.build.prob.stats(
      data.all = data.all,
      cols.stats = cols.stats,
      ids.tr = data.all$ID[as.character(data.all$SearchType) %chin% c("hist", "tr", "val")],
      ids.test = data.all$ID[as.character(data.all$SearchType) %chin% "test"],
      use.slide = F
    )
    setkey(data.prob.stats.test, ID)
    test.ix <- which(data.prob.stats$ID %in% data.prob.stats.test$ID)
    if (!all(data.prob.stats$ID[test.ix] == data.prob.stats.test$ID)) {
      stop('Ids dont match')
    }
    data.prob.stats[test.ix, n:= data.prob.stats.test$n]
    data.prob.stats[test.ix, pos:= data.prob.stats.test$pos]
    rm(data.prob.stats.test)
    invisible(gc())
    
    cat(" - done. k fitting started...\n")
    setkeyv(data.prob.stats, "ID")
    fn.check.id(data.prob.stats, data.prob.all)
    
    all.ix <- sort(unique(c(tr.ix, val.ix)))
    rm(tr.ix, val.ix, test.ix)
    
    data.click.mean <- mean(data.prob.all$IsClick[all.ix], na.rm=T)
    # calculate smoothed averages
    fn.opt.prob.cal <- function(pos, n, k, avg=data.click.mean)
        {round((pos + k*avg)/(n+max(c(0, min(c(k.max, k))))), digits=6)}
    fn.opt.prob.loss <- function(pars) {
      fn.log.loss(data.prob.all$IsClick[all.ix], 
                  fn.opt.prob.cal(pos=data.prob.stats$pos[all.ix],
                               n=data.prob.stats$n[all.ix],
                               k=pars)) 
    }
    if (is.null(k.opt)) {
      best.k <- optim(10, control = list(trace = F), fn.opt.prob.loss)
    } else {
      k.loss <- sapply(k.opt, fn.opt.prob.loss)
      k.ix <- which.min(k.loss)
      best.k <- list(par=k.opt[k.ix], value=k.loss[k.ix])
    }
    cat(col.target, 'best k:', best.k$par, ', loss:', best.k$value, '\n')
    data.prob.stats[, Pred := fn.opt.prob.cal(pos=pos, n=n, k=best.k$par)]
    data.prob.all[, col.target := data.prob.stats$Pred, with=F]
    rm(data.prob.stats)
    invisible(gc())
  }
  invisible(gc())
  data.prob.all[, IsClick := NULL]
  return (data.prob.all)
}


##############################################################
## builds list nway interaction
##############################################################
fn.build.interaction <- function(cols.in, ...) {
  interact.list <- list(...)
  list.res <- list()
  for (itrct in interact.list) {
    grid.params <- list()
    for (col.patt in sort(itrct)) {
      grid.params[[length(grid.params)+1]] <- sort(cols.in[grepl(col.patt, cols.in)])
    }
    grid.params <- unique(expand.grid(grid.params, stringsAsFactors = F))
    ix <- apply(grid.params, 1, function(x) !any(duplicated(x)))
    grid.params <- grid.params[ix,]
    ix <- !duplicated(apply(grid.params, 1, 
                            function(x) paste(sort(x), collapse=" - ")))
    grid.params <- grid.params[ix,]
    for (r in 1:nrow(grid.params)) {
      list.res[[length(list.res)+1]] <- as.character(grid.params[r,])
    }
  }
  list.res
}

##############################################################
## creates xgb matrix
##############################################################
fn.xgb.matrix <- function(data, subset, col.in, missing = -1) {
  suppressMessages(library("xgboost"))
  label <- data$IsClick[subset]
  if (any(is.na(label))) {
    xgb.DMatrix(
      data = as.matrix(data[subset, cols.in, with=F]), 
      missing = missing)
  } else {
    xgb.DMatrix(
      data = as.matrix(data[subset, cols.in, with=F]), 
      label = label,
      missing = missing)
  }
}

##############################################################
## checks search ids
##############################################################
fn.check.searchid <- function(...) {
  val.last <- NULL
  for (val.cur in list(...)) {
    if (nrow(val.cur) != 112159462) {
      stop('data doesnt have 112159462 rows')
    }
    if ( is.unsorted(val.cur$SearchID, strictly=T)) {
      stop('SearchID not sorted')
    }
    if (!is.null(val.last)) {
      if (!all(val.last$SearchID == val.cur$SearchID)) {
        stop('number or rows doesnt match')
      }
    }
    val.last <- val.cur
  }
}

##############################################################
## checks ids
##############################################################
fn.check.id <- function(...) {
  val.last <- NULL
  for (val.cur in list(...)) {
    if (nrow(val.cur) != 197974096) {
      stop('data doesnt have 197974096 rows')
    }
    if ( is.unsorted(val.cur$ID)) {
      stop('ID not sorted')
    }
    if (!is.null(val.last)) {
      if (!all(val.last$ID == val.cur$ID)) {
        stop('number or rows doesnt match')
      }
    }
    val.last <- val.cur
  }
}