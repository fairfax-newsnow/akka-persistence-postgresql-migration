#!/bin/bash

find $HOME/.sbt -name "*.lock" | xargs rm
find $HOME/.ivy2 -name "ivydata-*.properties" | xargs rm

# post-thread command -- stores the cache
if [ -e $HOME/.sbt ]
  then
  echo "$HOME/.sbt found so cache it"
  echo "ls -l $HOME/.sbt/"
  ls -l $HOME/.sbt/
  echo "-------------------------------------------"
  echo "cp -r $HOME/.sbt $SEMAPHORE_CACHE_DIR/"
  cp -R $HOME/.sbt $SEMAPHORE_CACHE_DIR/
  echo "-------------------------------------------"
  echo "ls -l $SEMAPHORE_CACHE_DIR/.sbt/ || true"
  ls -l $SEMAPHORE_CACHE_DIR/.sbt/ || true
fi
