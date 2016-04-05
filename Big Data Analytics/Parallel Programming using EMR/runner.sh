#!/bin/bash
######################################################################
# Answer script for Project 1 module 2                             ###
# Fill  in the functions below for each question.                  ###
# You may use any other files/scripts/languages                    ###
# in these functions as long as they are in the submission folder. ###
######################################################################

# The filtered data should be put in a file named ‘output’ 


# How many lines emerged in your output files?
# Run your commands/code to process the output file and echo a 
# single number to standard output
answer_1() {
	# Write a function to get the answer to Q1. Do not just echo the answer.
	#!/bin/bash
	#show the lines of output without printing file name
	wc -l < output
}

# What was the least popular article in the filtered output? How many total views
# did the least popular article get over the month?
# Run your commands/code to process the output and echo <article_name>\t<total views>
# to standard output.
# Break ties in reverse alphabetic order (if "ABC" and "XYZ" both have the minimum views, return "XYZ")
answer_2() {
	# Write a function to get the answer to Q2. Do not just echo the answer.
	#!/bin/bash
	# sort on numeric view first, then reversely sort article, print the first line 
	cat output | sort -k1,1n -rk2,2 | awk 'NR==1 {print $2"\t"$1}'
}

# What was the most popular article on December 18, 2015 from the filtered output? 
# How many daily views did the most popular article get on December 18?
# Run your commands/code to process the output and echo <article_name>\t<daily views>
# to standard output
answer_3() {
        # Write a function to get the answer to Q3. Do not just echo the answer.
        # sort and use substring to get the most popular movie's view on 18th
	cat output | java Q3     	 
}

# What is the most popular article of December 2015 with ZERO views on December 1, 2015?
# Run your commands/code to process the output and echo the answer
answer_4() {
        # Write a function to get the answer to Q4. Do not just echo the answer.
	# sort by whole views then use java biginteger to handle view number
#       cat output | sort -nr | awk '$3=="20151201:0" {print $2}' | awk 'NR==1 {print}'
#	cat output | sort -nrk1 -rk2 | java Q4
	cat output | java Q4
}

# For how many days over the month was the page titled "Twitter" more popular 
# than the page titled "Apple_Inc." ?
# Run your commands/code to process the dataset and echo a single number to standard output
# Do not hardcode the articles, as we will run your code with different articles as input
# For your convenience, "Twitter" is stored in the variable 'first', and "Apple_Inc." in 'second'.
answer_5() {
	# do not change the following two lines
	first=$(head -n 1 q5) #Twitter
	second=$(cat q5 | sed -n 2p) #Apple_Inc.
        # search by the second colomn, page title
        # search by the second colomn, page title
        cat output | awk -v first=$first '$2==first {print $0}' > firstTemp
        cat output | awk -v second=$second '$2==second {print $0}' > secondTemp
        day=1
        biggerDay=0
        # compare view on each day to find the bigger view number
        while [ $day -le 31 ]
        do
		TwitterView=$(cat firstTemp | awk -v day=$day '{print $(day+2)}')
	        AppleView=$(cat secondTemp | awk -v day=$day '{print $(day+2)}')
          # if Twitter is bigger, add one
          	if [ ${TwitterView:9} -gt ${AppleView:9} ]
          	then
    	            ((biggerDay++))
          	fi    
        ((day++))
        done 
        rm firstTemp
        rm secondTemp
        echo $biggerDay
}

# Rank the movie titles in the file q6 based on their maximum single-day wikipedia page views 
# (In descending order of page views, with the highest one first):
# Jurassic_Park_(film),The_Hunger_Games_(film),Fifty_Shades_of_Grey_(film),The_Martian_(film),Interstellar_(film)
# Ensure that you print the answers comma separated (As shown in the above line) without spaces
# For your convenience, code to read the file q6 is given below. Feel free to modify.
answer_6() {
	# Write a function to get the answer to Q6. Do not just echo the answer.
        #!/bin/bash
	while read line
        do
                # find the film in dataset and calculate the biggest view
                max=0
                day=1
                cat output | awk -v line=$line '$2==line {print $0}' > temp
                while [ $day -le 31 ]
                do
                    # ":" is to get the substring after date:, first index is 0
                    view=$(cat temp | awk -v day=$day '{print $(day+2)}')
                    if [ ${view:9} -gt $max ]
                    then
                    max=${view:9}
                    fi
                ((day++))
                done
                # save all films and biggest views into one file
        echo $max $line >> singleDay
        done < q6
        # give a print pattern, use sed to delete the last ","
        cat singleDay | sort -nr | awk -v ORS=, '{ print $2 }'| sed 's/,$//'
        rm temp
        rm singleDay
}

# Rank the operating systems in the file q7 based on their total month views page views
# (In descending order of page views, with the highest one first. In descending alphabetical order by name
# if the pageviews are same;
# OS_X,Android_(operating_system),Windows_10,Linux
# Ensure that you print the answers comma separated (As shown in the above line)
# For your convenience, code to read the file q7 is given below. Feel free to modify.
answer_7() {
	# Write a function to get the answer to Q7. Do not just echo the answer.
	#!/bin/bash
        while read line
        do
        	op=$line
                # get the system name and print view/name
		cat output | awk -v op=$op '$2==op {print $1"\t"$2}' >> temp
        done < q7
	#sort by decsending view and name alphabetical reversely
        cat temp | sort -nrk1 -rk2 | awk -v ORS=, '{ print $2 }' | sed 's/,$//'
        rm temp
}

# How many films in the dataset also have a corresponding TV series?
# Films are named <article_name>_([year_]film) 
# TV_series are named <article_name>_([year]_TV_series)
# 1. The article_name must be identical in both the film and TV_series
# 2. The film or TV_series *may* be accompanied by a 4 digit year
# Examples of valid cases:
# a. Concussion_(2015_film) is a match for Concussion_(TV_series)
# b. Scream_Queens_(2015_TV_series) is a match for Scream_Queens_(1929_film)
# Run your commands/code to process the output and echo the answer
answer_8() {
        #!/bin/bash
	# use $ to find the film with "film)" and use "(" to get the name part
	awk '$2 ~ /film\)$/ {print $2}' output | awk -F"(" '{print $1}' | sort > filmFile
	# use $ to find the series with "film)" and use "(" to get the name part
	awk '$2 ~ /TV\_series\)$/ {print $2}' output | awk -F"(" '{print $1}' | sort > seriesFile
	while read line
	do
    		film=$line
   		while read line
    		do
    		# use while loop to determine whther two titles equal or not
    		awk -v film=$film '$1==film {print}' >> temp
    		done < seriesFile
	done < filmFile
	# return lines of temp files, the number films which have series
	wc -l < temp
	rm temp
	rm filmFile
	rm seriesFile
}

# Find out the number of articles with longest number of strictly decreasing sequence of views
# Example: If 21 articles have strictly decreasing pageviews everyday for 5 days (which is the global maximum), 
# then your script should find these articles from the output file and return 21.
# Run your commands/code to process the output and echo the answer
answer_9() {
        # Write a function to get the answer to Q9. Do not just echo the answer.
	#!/bin/bash
	cat output | java Q9	
#	global=0
#	globalnum=0
#	while read line
#	do
	# exclude empty line situation
#	if [ "$line" != "" ]; then
 # 		max=0
  #		curMax=0
  #		day=1
  #		curView=0
  		# loop in a month
  #		while [ $day -le 31 ]
  #		do
   #  		view=$(echo $line | awk -v day=$day '{print $(day+2)}')
      		# if view is less than previouse day, curren descending day 
      		# add one else it turns back to 1
    #  		if [ ${view:9} -lt $curView ]
     # 		then
      #  		((curMax++))
      #		elif [ ${view:9} -gt $curView ]
      #		then
       # 		curMax=0
      	#	fi
      		# if current max is bigger, global max gets this value
      	#	if [ $curMax -gt $max ]
      	#	then
        #		max=$curMax
      	#	fi
      	#	curView=${view:9}
      	#	((day++))
  	#done
  	# compare current max and global max
  #	if [ $max -eq $global ]
  #	then
   # 		((globalnum++))
  #	fi
  #	if [ $max -gt $global ]
  #	then
   # 		global=$max
    #		globalnum=1
  #	fi
#	fi
#	done < output
#	echo $globalnum
}

# What was the type of the master instance that you used in EMR
# Ungraded question
answer_10() {
    # echo the answer (instance type)
    echo "m3.xlarge"
}

# What was the type of the task/core instances that you used in EMR
# Ungraded question (this means you don't get points for answering, but we deduct points if you don't answer honestly)
answer_11() {
    # echo the answer (instance type)
    echo "m3.xlarge"
}

# How many task/core instances did you use in your EMR run?
# Ungraded question (this means you don't get points for answering, but we deduct points if you don't answer honestly)
answer_12() {
    # echo the answer (instance count)
    echo "10"
}

# What was the execution time of your EMR run? (You can find this in the EMR console on AWS)
# Please echo the number of minutes your job took
# Ungraded question (this means you don't get points for answering, but we deduct points if you don't answer honestly)
answer_13() {
	# echo the answer (execution time in minutes)
	echo "29"
}

# DO NOT MODIFY ANYTHING BELOW THIS LINE
echo "Checking the files: "
if [ -f 'output' ]
then
        echo output file created, good!
else
        echo No output file created
fi

echo "The results of this run are : "
echo "{"

a1=`answer_1`
echo -en ' '\"answer1\": \"$a1\"
echo $a1 > .1.out
echo ","

a2=`answer_2`
echo -en ' '\"answer2\": \"$a2\"
echo $a2 > .2.out
echo ","

a3=`answer_3`
echo -en ' '\"answer3\": \"$a3\"
echo $a3 > .3.out
echo ","

a4=`answer_4`
echo -en ' '\"answer4\": \"$a4\"
echo $a4 > .4.out
echo ","

a5=`answer_5`
echo -en ' '\"answer5\": \"$a5\"
first=$(head -n 1 q5)
second=$(cat q5 | sed -n 2p)
echo "$first,$second,$a5" > .5.out
echo ","

a6=`answer_6`
echo -en ' '\"answer6\": \"$a6\"
echo $a6 > .6.out
echo ","

a7=`answer_7`
echo -en ' '\"answer7\": \"$a7\"
echo $a7 > .7.out
echo ","

a8=`answer_8`
echo -en ' '\"answer8\": \"$a8\"
echo $a8 > .8.out
echo ","

a9=`answer_9`
echo -en ' '\"answer9\": \"$a9\"
echo $a9 > .9.out
echo ","

a10=`answer_10`
echo -en ' '\"answer10\": \"$a10\"
echo $a10 > .10.out
echo ","

a11=`answer_11`
echo -en ' '\"answer11\": \"$a11\"
echo $a11 > .11.out
echo ","

a12=`answer_12`
echo -en ' '\"answer12\": \"$a12\"
echo $a12 > .12.out
echo ","

a13=`answer_13`
echo -en ' '\"answer13\": \"$a13\"
echo $a13 > .13.out
 
echo  "}"

echo ""
echo "If you feel these values are correct please run:"
echo "./submitter -a andrew_id -l <python,java>"

