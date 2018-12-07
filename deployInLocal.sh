rm -rf build-files && \
rm -rf $HOME/ecommerceanalytics
mvn clean package -DskipTests=true && \
echo "Completed packaging, deploying to Local" && \
mkdir -p $HOME/ecommerceanalytics && \
cp -r build-files/* $HOME/ecommerceanalytics && \
echo "Deployed in Local"

exit 0
