DMD=dmd

rm *.a
t_src=src/trioplax

git log -1 --pretty=format:"module myversion; public static char[] author=cast(char[])\"%an\"; public static char[] hash=cast(char[])\"%h\";">myversion.d

$DMD -m64 -d -Iimport \
$t_src/Logger.d $t_src/triple.d $t_src/TripleStorage.d \
src/tango/text/convert/Integer.d src/tango/core/Exception.d \
$t_src/mongodb/TripleStorageMongoDB.d $t_src/mongodb/ComplexKeys.d myversion.d -O -Hdexport/trioplax -release -lib -oftrioplax64

rm *.o