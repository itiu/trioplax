module trioplax.triple;
class Triple
{
    char[] s;
    char[] p;
    char[] o;
    
    byte lang;
}

class triple_list_element
{
    Triple triple;
    triple_list_element next_triple_list_element;
}
