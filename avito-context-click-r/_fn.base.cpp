#include <Rcpp.h>
#include <math.h>

#define MIN(a, b) ((a < b) ? a : b)
#define MAX(a, b) ((a > b) ? a : b)
#define BOUND(x, xmin, xmax) (MAX(xmin, MIN(xmax, x)))
#define LOG_LOSS(act, pred) (-(act * log(pred) + (1 - act) * log(1 - pred)))

using namespace Rcpp;


double fn_opt_base(NumericMatrix x, NumericVector y, NumericVector params, int type) {
    
  int rows = x.nrow();
  int cols = x.ncol();
  
  int offset_params = 0;
  double bias = 0.0;
  if (params.size() > cols) {
    offset_params = 1;
    bias = params[0];
  }
  
  double pred_max = 1.0 - 1e-15;
  double pred_min = 1e-15;
  
  double total_err = 0.0;
  for (int r = 0; r < rows; ++r) {
    
    double y_pred = (type==2)? 1.0 : 0.0;
    for (int c = 0; c < cols; ++c) {
      if (type==2) {
        y_pred *= pow(x(r,c), params[c+offset_params]);
      } else {
        y_pred += x(r,c)*params[c+offset_params];
      }
    }
    y_pred += bias;
    y_pred = BOUND(y_pred, pred_min, pred_max);
    total_err += LOG_LOSS(y[r], y_pred);
  }
  
  return total_err/(double)rows;
}


// [[Rcpp::export]]
double fn_opt_gm(NumericMatrix x, NumericVector y, NumericVector params) {
    return fn_opt_base(x, y, params, 2);
}

// [[Rcpp::export]]
double fn_opt_am(NumericMatrix x, NumericVector y, NumericVector params) {
    return fn_opt_base(x, y, params, 1);
}
