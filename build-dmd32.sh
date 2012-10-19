DMD=dmd

#mongo-d-driver 95f4aef

rm trioplax32.a
t_src=src/trioplax

git log -1 --pretty=format:"module myversion; public static char[] author=cast(char[])\"%an\"; public static char[] hash=cast(char[])\"%h\";">myversion.d

$DMD -m32 -Iimport $t_src/Logger.d $t_src/triple.d $t_src/TripleStorage.d \
$t_src/mongodb/TripleStorageMongoDB.d $t_src/mongodb/ComplexKeys.d myversion.d -O -Hdexport/trioplax -release -lib -oftrioplax32

rm *.o
