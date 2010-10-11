rm *.a
src=src/trioplax/memory
general_src=src/trioplax

git log -1 --pretty=format:"module myversion; public final static char[] author=\"%an\"; public final static char[] hash=\"%h\";">myversion.d

dmd -Iimport/libmongod $general_src/Log.d $general_src/triple.d $general_src/TripleStorage.d $src/Hash.d $src/TripleHashMap.d $src/IndexException.d $src/TripleStorageMemory.d \
$general_src/mongodb/TripleStorageMongoDB.d myversion.d -O -Hdexport/trioplax -release -lib -oftrioplax


rm *.o