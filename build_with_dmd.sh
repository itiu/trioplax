#mongo-d-driver 98aad67

rm *.a
src=src/trioplax/memory
general_src=src/trioplax

git log -1 --pretty=format:"module myversion; public static char[] author=cast(char[])\"%an\"; public static char[] hash=cast(char[])\"%h\";">myversion.d

~/dmd/linux/bin/dmd -version=D1 -Iimport/libmongod $general_src/Log.d $general_src/triple.d $general_src/TripleStorage.d $src/Hash.d $src/TripleHashMap.d $src/IndexException.d $src/TripleStorageMemory.d \
$general_src/mongodb/TripleStorageMongoDB.d myversion.d -O -Hdexport/trioplax -release -lib -oftrioplax-D1

~/dmd2/linux/bin/dmd -version=D2 -Iimport/libmongod $general_src/Log.d $general_src/triple.d $general_src/TripleStorage.d $src/Hash.d $src/TripleHashMap.d $src/IndexException.d $src/TripleStorageMemory.d \
$general_src/mongodb/TripleStorageMongoDB.d myversion.d -O -Hdexport/trioplax -release -lib -oftrioplax-D2

rm *.o