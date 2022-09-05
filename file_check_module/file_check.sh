#!/usr/bin/env bash
echo 'Checking for new files...'

search_dir=`ls $1/*`
correct_ext=()
processed= '_processed'

for file in $search_dir
do
   if [[ $file == *"$processed"* ]]
   then  
      if [[ $file == *.csv ]]
      then
         
         if [[ `wc -l $file` > 0 ]]
         then

            echo "here is where data_check.py is executed with the related file."

            mv $file $file"_processed.csv"
            correct_ext+=($file)
         
         else
            echo $file "is empty."
         fi

      else
         echo $file "is a not csv file."
      fi

   else
      echo $file "is already processed."
   fi
done
