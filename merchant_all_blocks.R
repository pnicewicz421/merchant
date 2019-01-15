# merchant

library(keras)
library(data.table)
library(gdata)

# load datasets
train <- fread("train.csv")
test <- fread("test.csv")

# load additional files
hist <- fread("historical_transactions.csv")
new <- fread("new_merchant_transactions.csv")
merchants <- fread("merchants.csv")


# 2.2 preprocess historical transactions data
# the data will be preprocessed in batches 
# during the generator

# transform each categorical field into ranks
# and shift the rank starting from 1 to 0 
# to then transform them into categorial variables

# create a list of dim 2 to mimic a dictionary
# the sorted unique values found in each categorical variable
# will be transformed to their unique sorted ranks (using the dense
# method for ties) and both the original and transformed values
# will be preserved {key:value}

# create rank maps for categorical variables in history
# and new transactions
CreateRankedMap <- function(categorical_variables) {
  
  for (var_pck in categorical_variables) { 
    
    var <- var_pck[1]
    datafile <- var_pck[2:length(var_pck)]
    
    #combine the results from multiple files
    key <- c()
    for (i in datafile) {
      key_local <- sort(unique(get(i)[, get(var)]), na.last=FALSE)
      key <- c(key, key_local)
    }
    
    key <- unique(key)
    
    value <- frank(key, ties.method="dense", na.last=FALSE)
    value <- value - 1

    assign(paste0(var), data.table(key, value), envir = .GlobalEnv)
    print (paste0(var, " assigned"))
  }
  return ("Done ...")
}

categorical_variables <- list(c("authorized_flag", "hist", "new"),
                           c("city_id", "hist", "new"),
                           c("category_1", "hist", "new"),
                           c("category_2", "hist", "new"),
                           c("category_3", "hist", "new"),
                           c("merchant_category_id", "hist", "new"),
                           c("subsector_id", "hist", "new"),
                           c("state_id", "hist", "new"),
                           c("category_4", "merchants"),
                           c("feature_1", "train", "test"),
                           c("feature_2", "train", "test"),
                           c("feature_3", "train", "test"),
                           c("most_recent_sales_range", "merchants"),
                           c("most_recent_purchases_range", "merchants"))
                           
CreateRankedMap(categorical_variables)
  
# remove duplicative columns
# subsector_id will be taken from the purchases group
# because it is a richer dataset (I think)
# we might need to revisit this
merchants[,merchant_category_id:=NULL]
merchants[,subsector_id:=NULL]
merchants[,category_1:=NULL] # YEEESSS 
merchants[,category_2:=NULL]

# get rid of merchant city, and state (I think that's ok)
merchants[,city_id:=NULL]  # YEEESSS 
merchants[,state_id:=NULL]

merchants[,merchant_group_id:=NULL]

# get rid of the merchant_group_id,
# because it is too large 
# the merchant category id seems to be more confined


# for continuous variables, 
# find the mean and standard deviation of the train set
# and normalize
#purchase_amount, month_lag, purchase_date

# change purchase dates into numeric
#hist_dates <- hist$purchase_date
#new_dates <- new$purchase_date
#hist$purchase_date <- as.numeric(as.POSIXct(hist_dates))
#new$purchase_date <- as.numeric(as.POSIXct(new_dates))

hist_records <- 1:nrow(hist)
new_records <- (nrow(hist) + 1): (nrow(hist) + nrow(new))

records <- c(hist_records, new_records)

train_records <- 
 records[c(which(hist$card_id %in% train$card_id),
    (which(new$card_id %in% train$card_id) + nrow(hist)))]

# split up the training set into training and validation
split_ratio <- 0.8
samp <- as.integer(length(train_records)*split_ratio)
train_indices <- sample(train_records, samp, replace=FALSE)
val_indices <- train_records[-which(train_indices %in% train_records)]

# test set is already accounted for
test_indices <- 1:nrow(test)

# slightly revised plan:
# we need to normalize continuous variables 
# by using the mean and sd from the training set only
# and applying that mean and sd for all sets...

# so from now on, rather than having a hist and new
# we will have train, val, and test .. .can we do that?

hist$rownumber <- 1:nrow(hist)
new$rownumber <- (nrow(hist) + 1): (nrow(hist) + nrow(new))

# put together the records from the training dataset from the hist and new 
# dataset
train_indices_hist <- train_indices[train_indices <= nrow(hist)]
train_indices_hist <- data.table(train_indices_hist)
colnames(train_indices_hist) <- "rownumber"

train_indices_new <- train_indices[train_indices > nrow(new)]
train_indices_new <- data.table(train_indices_new)
colnames(train_indices_new) <- "rownumber"

merge1 <- merge(x = train_indices_hist, y = hist, by = "rownumber", all.x = TRUE)
merge2 <- merge(x = train_indices_new, y = new, by = "rownumber", all.x = TRUE)
train_hist <- rbind(merge1, merge2)
#train_hist <- train_hist[!is.null(card_id),]

# merge train_hist with train, right?
trainset <- merge(train_hist, train, by = "card_id", all.x=TRUE)

# merge with merchants  
trainset1 <- merge(trainset, merchants, by = "merchant_id", all.x=TRUE)

#remove the places that did not match with a merchant_id
###############TRAINING SET##################
trainset1 <- trainset1[merchant_id!="",]


### VALIDATION SET#################
# put together the records from the val dataset from the hist and new 
# dataset
val_indices_hist <- val_indices[val_indices <= nrow(hist)]
val_indices_hist <- data.table(val_indices_hist)
colnames(val_indices_hist) <- "rownumber"

val_indices_new <- val_indices[val_indices > nrow(hist)]
val_indices_new <- data.table(val_indices_new)
colnames(val_indices_new) <- "rownumber"

merge1 <- merge(x = val_indices_hist, y = hist, by = "rownumber", all.x = TRUE)
merge2 <- merge(x = val_indices_new, y = new, by = "rownumber", all.x = TRUE)
val_hist <- rbind(merge1, merge2)

# merge with the train set
valset <- merge(val_hist, train, by = "card_id", all.x=TRUE)

# merge with merchants  
valset1 <- merge(valset, merchants, by = "merchant_id", all.x=TRUE)

#remove the places that did not match with a merchant_id
valset1 <- valset1[merchant_id!="",]

##### TESTING SET #################
test_indices_hist <- test_indices[test_indices <= nrow(hist)]
test_indices_hist <- data.table(test_indices_hist)
colnames(test_indices_hist) <- "rownumber"

test_indices_new <- test_indices[test_indices > nrow(hist)]
test_indices_new <- data.table(test_indices_new)
colnames(test_indices_new) <- "rownumber"

merge1 <- merge(x = test_indices_hist, y = hist, by = "rownumber", all.x = TRUE)
merge2 <- merge(x = test_indices_new, y = new, by = "rownumber", all.x = TRUE)
test_hist <- rbind(merge1, merge2)

testset <- merge(test_hist, test, by = "card_id", all.x=TRUE)

# merge with merchants  
testset1 <- merge(testset, merchants, by = "merchant_id", all.x=TRUE)

#remove the places that did not match with a merchant_id
testset1 <- testset1[merchant_id!="",]

# Notice that there will be one fewer column in testset1
# (33) than valset1 and trainset1 (34). That's because
# target is not included in the testset.

outputs <- c()

continuous_variables <- c("numerical_1",
                         "numerical_2",
                         "avg_sales_lag3",
                         "avg_purchases_lag3",
                         "active_months_lag3",
                         "avg_sales_lag6",
                         "avg_purchases_lag6",
                         "active_months_lag6",
                         "avg_sales_lag12",
                         "avg_purchases_lag12",
                         "active_months_lag12",
                         "installments",
                         "month_lag",
                         "purchase_amount",
                         "purchase_date",
                         "first_active_month")

for (var in continuous_variables){
  
  # only get one variable at a time
  output <- trainset1[,get(var)]
  
  if (typeof(output)=="character") {
    # encountered a character -- in this case, date format
    # for purchase date
    if (nchar(output)[1] > 10) { 
      output <- substr(output, 1, 10)
      output <- as.numeric(as.Date(output))
    } else if (nchar(output)[1] == 7) {
      # this is for the first purchase month in the train
      output_month <- substr(output, 1, 4)
      output_day <- substr(output, 6, 7)
      output <- paste0(output_month, output_day)
      output <- as.numeric(output)
    }
    
  }
  
  # get the mean and standard deviation 
  # and set aside in a separate variable
  assign(paste0(eval(var), "_mean"), mean(output))
  assign(paste0(eval(var), "_sd"), sd(output))
  
  output <- (output - get(paste0(eval(var), "_mean"))) / 
    get(paste0(eval(var), "_sd"))
  
  # assign normalized values to the variable in trainset1
  trainset1[,eval(var):=output] 
  
  # normalize the value on the spot 
  # (value - mean) / sd
  counter <- 1 
  datafile <- c("valset1", "testset1")
  for (dt in datafile) {
    # for train, validation, and test,
    # fill in the normalized output for that column
    get(dt)[,eval(var):=output[counter:(nrow(get(dt))+counter-1)]]
    counter <- counter + nrow(get(dt))
  }

}


# set up generator
generator <- function(dataset, batch_size=128){
  # dataset is either
  
  # dataset = "train", "val" or test"
  
  # since the overall dataset is too large,
  # join the data in the generator
  
  # the categorical variables already have been 
  # one-hot encoded in the train set

  index <- 1
  
  #dates <- c(hist$purchase_date, new$purchase_date)
  
  function() {
    # pick rows in order
    #batch_indices <- data_indices[index:(index+batch_size-1)]
    
    batch <- dataset[index:(index+batch_size-1),]

    
    # remove merchant_id, card_id
    batch[,merchant_id:=NULL]
    batch[,card_id:=NULL]
    
  
    
    categorical_variables <- c("authorized_flag",
                                    "city_id",
                                    "category_1",
                                    "category_3",
                                    "merchant_category_id",
                                    "category_2",
                                    "state_id",
                                    "subsector_id",
                                    "feature_1",
                                    "feature_2",
                                    "feature_3",
                                    "category_4")
      
      # for each categorical variable, create a 
      # join with categorical variables
      for (var in categorical_variables) {
        # outer join on all card_ids, the 
        # data tablvare with keys to the original values 
        # and their mapping in ranked order (starting at 0)
        # rename the value field to the variable name
        # remove the key field (since we will no longer need it)
        # one-hot encode the ordered values 
        # bind the columns of the one-hot encoder with the rest of the batch
        
        # specify which data file the variable is from -- 
        print(paste0("Now Trying: ", var))
        
        batch <- merge(get(var), batch,
                       by.x="key", by.y=eval(var),
                       all.y=TRUE) 
        
        # rename the value column the variable name
        colnames(batch)[colnames(batch)=="value"] = var
        
        # remove the key column
        batch[,key:=NULL]
        # need only to one-hot code encode for over 2 variables
        # 2 variables can be in one
        if (nrow(get(var))>2) {
          #create a separate data frame with for the categorical variable
          
          assign(paste0(var, "_enc"), to_categorical(batch[ , get(var)],dim(get(var))[1]))
          
          # join that data frame with the batch data
          batch <- cbind(batch, var=get(paste0(var, "_enc")))
          
          # rename to original values of the categorical
          for (num in 1:nrow(get(var))) {
            colnames(batch)[colnames(batch)==paste0("var.V", as.character(num))] = paste0(var, ".", get(var)[value==as.integer(num)-1, key])
          }
          # remove the column name with the mapped value and the mapping data frame
          batch[,eval(var):=NULL]
          rm(list=paste0(eval(var), "_enc"))
        } 
        # assign the original batch (either purchases or batch)
        # the newly formed sm_batch
        
        print(paste0("Finished Successfully: ", var))       
      }
