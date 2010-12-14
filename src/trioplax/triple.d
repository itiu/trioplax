module trioplax.triple;
struct Triple
{
    char[] s;
    char[] p;
    char[] o;
    
    byte lang;
}

struct triple_list_element
{
    Triple* triple;
    triple_list_element* next_triple_list_element;
}
