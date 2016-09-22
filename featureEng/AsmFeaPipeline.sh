sed 's/$/.asm/' X_train.txt > tmpfile.txt # add .bytes to tail of each line, and save the tmp file
sed -e 's#^#https://s3.amazonaws.com/eds-uga-csci8360/data/project2/metadata/#' tmpfile.txt > trainAsmPath.txt # add 'http' address to head of each line
mkdir asmdata
mkdir pngdata
wget -i trainAsmPath.txt -P ./asmdata/
asmpath=./asmdata
pngpath=./pngdata
python2 asmFeature.py $asmpath $pngpath

