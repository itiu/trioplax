DMD=dmd

#mongo-d-driver 95f4aef

rm *.a
t_src=src/trioplax

git log -1 --pretty=format:"module myversion; public static char[] author=cast(char[])\"%an\"; public static char[] hash=cast(char[])\"%h\";">myversion.d

$DMD -d -version=dmd2_053 -version=D2 -Iimport/libmongod $t_src/Logger.d $t_src/triple.d $t_src/TripleStorage.d \
$t_src/memory/*.d \
src/rt/util/hash.d \
src/tango/text/convert/Integer.d src/tango/core/Exception.d \
$t_src/mongodb/TripleStorageMongoDB.d myversion.d -O -Hdexport/trioplax -release -lib -oftrioplax-D2

rm *.o