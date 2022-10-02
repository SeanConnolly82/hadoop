
library("ggplot2")
library("dplyr")

# turn off scientific notation for the axes labels
options(scipen = 999)
# working directory for hadoop outputs
setwd("~/Library/Mobile Documents/com~apple~CloudDocs/MSc Computer Science/Programming for Big Data/Assignment A")
# load Job2 output
hadoop_letters <- read.delim("job2_output.txt", header = FALSE, sep = "\t")
# load Job3 output
hadoop_totals <- read.delim("job3_output.txt", header = FALSE, sep = "\t")
# headers for Job2 output
letter_headers <- c("language","letter","letter_count")
# assign headers
colnames(hadoop_letters) <- letter_headers
# headers for Job3 output
total_headers <- c("language", "total_count")
# assign headers
colnames(hadoop_totals) <- total_headers
# join the Job2 and Job3 outputs
hadoop <- merge(hadoop_letters, hadoop_totals, by="language")
# chart the raw Job2 data
ggplot(hadoop_letters, aes(fill=language, x=letter, y=letter_count)) +
geom_bar(position="dodge", stat="identity") +
labs(title = "Job2 Data - Absolute count", x="Letter", y="Letter count") + 
theme_minimal()
# filter the raw Job2 data for letters that have a count less than 100
hadoop_filtered <- filter(hadoop_letters, letter_count >= 100)
# chart the filtered Job2 data
ggplot(hadoop_filtered, aes(fill=language, x=letter, y=letter_count)) +
geom_bar(position="dodge", stat="identity") +
labs(title = "Job2 Data - Low counts filtered", x="Letter", y="Letter count") + 
theme_minimal()
# filter for french only
hadoop_french <- filter(hadoop, language == "french", letter_count >= 100)
# charts of french letters
ggplot(hadoop_french, aes(fill=language, x=letter, y=letter_count/total_count)) +
geom_bar(position="dodge", stat="identity", fill="coral2") +
labs(title = "French letter frequency", x="Letter", y="Letter frequency") + 
theme_minimal()
# filter for german only
hadoop_german <- filter(hadoop, language == "german", letter_count >= 100)
# charts of german letters
ggplot(hadoop_german, aes(fill=language, x=letter, y=letter_count/total_count)) +
geom_bar(position="dodge", stat="identity", fill="green3") +
labs(title = "German letter frequency", x="Letter", y="Letter frequency") + 
theme_minimal()
# filter for spanish only
hadoop_spanish <- filter(hadoop, language == "spanish", letter_count >= 100)
# charts of spanish letters
ggplot(hadoop_spanish, aes(fill=language, x=letter, y=letter_count/total_count)) +
geom_bar(position="dodge", stat="identity", fill="cornflowerblue") +
labs(title = "Spanish letter frequency", x="Letter", y="Letter frequency") + 
theme_minimal()
# charts of all letters frequencies
ggplot(filter(hadoop, letter_count >= 100), aes(fill=language, x=letter, y=letter_count/total_count)) +
geom_bar(position="dodge", stat="identity") +
labs(title = "All letters frequency - a comparison", x="Letter", y="Letter frequency") + 
theme_minimal()
