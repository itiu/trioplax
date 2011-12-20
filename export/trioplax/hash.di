// D import file generated from 'src/rt/util/hash.d'
module rt.util.hash;
version (X86)
{
    version = AnyX86;
}
version (X86_64)
{
    version = AnyX86;
}
version (AnyX86)
{
    version = HasUnalignedOps;
}
hash_t hashOf(const(void)* buf, size_t len, hash_t seed = 0);
