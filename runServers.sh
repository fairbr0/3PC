# Input paramter is the number of slaves. Default is 2
if [ $# -eq 0 ]
  then
    N=2
  else
    N=$1
fi

# Run the coordinator. An extra parameter N is passed which is the number of servers
java -cp .:TPC.jar Node coordinator 9001 9000 null db0.txt $N &

# Create n instances of slaves. Each slave is passed the coordinator address
for ((i=1; i<=N; i++)); do
  dbName="db$i.txt"
  java -cp .:TPC.jar Node slave null null localhost:9001 $dbName null &
done
