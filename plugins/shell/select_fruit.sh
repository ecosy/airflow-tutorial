FRUIT=$1
if [ $FRUIT == APPLE ]; then
      echo "you selected APPLE!"
elif [ $FRUIT == ORANGE ]; then
      echo "you selected ORANGE!"
else
      echo "you selected other fruit : $FRUIT"
fi