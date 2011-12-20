// D import file generated from 'src/tango/text/convert/Integer.d'
module tango.text.convert.Integer;
import tango.core.Exception;
version (X86_64)
{
    alias ulong length_t;
}
else
{
    alias int length_t;
}
template toInt(T,U = uint)
{
int toInt(T[] digits, U radix = 0)
{
return toInt!(T)(digits,radix);
}
}
template toInt(T)
{
int toInt(T[] digits, uint radix = 0)
{
auto x = toLong(digits,radix);
if (x > (int).max)
throw new IllegalArgumentException("Integer.toInt :: integer overflow");
return cast(int)x;
}
}
template toLong(T,U = uint)
{
long toLong(T[] digits, U radix = 0)
{
return toLong!(T)(digits,radix);
}
}
template toLong(T)
{
long toLong(T[] digits, uint radix = 0)
{
uint len;
auto x = parse(digits,radix,&len);
if (len < digits.length)
throw new IllegalArgumentException("Integer.toLong :: invalid literal");
return x;
}
}
char[] toString(long i, char[] fmt = null)
{
char[66] tmp = void;
return format(tmp,i,fmt).dup;
}
wchar[] toString16(long i, wchar[] fmt = null)
{
wchar[66] tmp = void;
return format(tmp,i,fmt).dup;
}
dchar[] toString32(long i, dchar[] fmt = null)
{
dchar[66] tmp = void;
return format(tmp,i,fmt).dup;
}
template format(T,U = long)
{
T[] format(T[] dst, U i, T[] fmt = null)
{
return format!(T)(dst,cast(long)i,fmt);
}
}
template format(T)
{
T[] format(T[] dst, long i, T[] fmt = null)
{
char pre,type;
int width;
decode(fmt,type,pre,width);
return formatter(dst,i,type,pre,width);
}
}
private template decode(T)
{
void decode(T[] fmt, ref char type, out char pre, out int width)
{
if (fmt.length is 0)
type = 'd';
else
{
type = cast(char)fmt[0];
if (fmt.length > 1)
{
char* p = cast(char*)&fmt[1];
{
for (int j = 1;
 j < fmt.length; ++j , ++p)
{
if (*p >= '0' && *p <= '9')
width = width * 10 + (*p - '0');
else
pre = *p;
}
}
}
}
}
}

template formatter(T,U = long,X = char,Y = char)
{
T[] formatter(T[] dst, U i, X type, Y pre, int width)
{
return formatter!(T)(dst,cast(long)i,type,pre,width);
}
}
private template _FormatterInfo(T)
{
struct _FormatterInfo
{
    uint radix;
    T[] prefix;
    T[] numbers;
}
}

template formatter(T)
{
T[] formatter(T[] dst, long i, char type, char pre, length_t width)
{
T[] lower = cast(T[])"0123456789abcdef";
T[] upper = cast(T[])"0123456789ABCDEF";
alias _FormatterInfo!(T) Info;
const Info[] formats = [{10,null,lower},{10,cast(T[])"-",lower},{10,cast(T[])" ",lower},{10,cast(T[])"+",lower},{2,cast(T[])"0b",lower},{8,cast(T[])"0o",lower},{16,cast(T[])"0x",lower},{16,cast(T[])"0X",upper}];
ubyte index;
length_t len = dst.length;
if (len)
{
switch (type)
{
case 'd':
{
}
case 'D':
{
}
case 'g':
{
}
case 'G':
{
if (i < 0)
{
index = 1;
i = -i;
}
else
{
if (pre is ' ')
index = 2;
else
if (pre is '+')
index = 3;
}
}
case 'u':
{
}
case 'U':
{
pre = '#';
break;
}
case 'b':
{
}
case 'B':
{
index = 4;
break;
}
case 'o':
{
}
case 'O':
{
index = 5;
break;
}
case 'x':
{
index = 6;
break;
}
case 'X':
{
index = 7;
break;
}
default:
{
return cast(T[])"{unknown format '" ~ cast(T)type ~ "'}";
}
}
auto info = &formats[index];
auto numbers = info.numbers;
auto radix = info.radix;
auto p = dst.ptr + len;
if ((uint).max >= cast(ulong)i)
{
auto v = cast(uint)i;
do
{
*--p = numbers[v % radix];
}
while ((v /= radix) && --len);}
else
{
auto v = cast(ulong)i;
do
{
*--p = numbers[cast(uint)(v % radix)];
}
while ((v /= radix) && --len);}
auto prefix = pre is '#' ? info.prefix : null;
if (len > prefix.length)
{
len -= prefix.length + 1;
if (width)
{
width = dst.length - width - prefix.length;
while (len > width && len > 0)
{
*--p = '0';
--len;
}
}
dst[len..len + prefix.length] = prefix;
return dst[len..$];
}
}
return cast(T[])"{output width too small}";
}
}
template parse(T,U = uint)
{
long parse(T[] digits, U radix = 0, uint* ate = null)
{
return parse!(T)(digits,radix,ate);
}
}
template parse(T)
{
long parse(T[] digits, uint radix = 0, uint* ate = null)
{
bool sign;
auto eaten = trim(digits,sign,radix);
auto value = convert(digits[eaten..$],radix,ate);
if (ate && *ate > 0)
*ate += eaten;
return cast(long)(sign ? -value : value);
}
}
template convert(T,U = uint)
{
ulong convert(T[] digits, U radix = 10, uint* ate = null)
{
return convert!(T)(digits,radix,ate);
}
}
template convert(T)
{
ulong convert(T[] digits, uint radix = 10, uint* ate = null)
{
uint eaten;
ulong value;
foreach (c; digits)
{
if (c >= '0' && c <= '9')
{
}
else
if (c >= 'a' && c <= 'z')
c -= 39;
else
if (c >= 'A' && c <= 'Z')
c -= 7;
else
break;
if ((c -= '0') < radix)
{
value = value * radix + c;
++eaten;
}
else
break;
}
if (ate)
*ate = eaten;
return value;
}
}
template trim(T,U = uint)
{
uint trim(T[] digits, ref bool sign, ref U radix)
{
return trim!(T)(digits,sign,radix);
}
}
template trim(T)
{
uint trim(T[] digits, ref bool sign, ref uint radix)
{
T c;
T* p = digits.ptr;
size_t len = digits.length;
if (len)
{
{
for (c = *p; len; c = *++p , --len)
{
if (c is ' ' || c is '\x09')
{
}
else
if (c is '-')
sign = true;
else
if (c is '+')
sign = false;
else
break;
}
}
auto r = radix;
if (c is '0' && len > 1)
switch (*++p)
{
case 'x':
{
}
case 'X':
{
++p;
r = 16;
break;
}
case 'b':
{
}
case 'B':
{
++p;
r = 2;
break;
}
case 'o':
{
}
case 'O':
{
++p;
r = 8;
break;
}
default:
{
--p;
break;
}
}
if (r is 0)
radix = 10;
else
if (radix != r)
{
if (radix)
p -= 2;
else
radix = r;
}
}
return cast(uint)(p - digits.ptr);
}
}
template atoi(T)
{
uint atoi(T[] s, int radix = 10)
{
uint value;
foreach (c; s)
{
if (c >= '0' && c <= '9')
value = value * radix + (c - '0');
else
break;
}
return value;
}
}
template itoa(T,U = uint)
{
T[] itoa(T[] output, U value, int radix = 10)
{
return itoa!(T)(output,value,radix);
}
}
template itoa(T)
{
T[] itoa(T[] output, uint value, int radix = 10)
{
T* p = output.ptr + output.length;
do
{
*--p = cast(T)(value % radix + '0');
}
while (value /= radix);return output[cast(size_t)(p - output.ptr)..$];
}
}
template consume(T)
{
T[] consume(T[] src, bool fp = false)
{
T c;
bool sign;
uint radix;
auto e = src.ptr + src.length;
auto p = src.ptr + trim(src,sign,radix);
auto b = p;
if (src.length is 0 || p > &src[$ - 1])
return null;
{
for (c = *p; p < e && (c >= '0' && c <= '9' || radix is 16 && (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'));)
{
c = *++p;
}
}
if (fp)
{
if (c is '.' && p < e)
c = *++p;
while (c >= '0' && c <= '9' && p < e)
c = *++p;
if (p > b)
{
if ((c is 'e' || c is 'E') && p < e)
{
c = *++p;
if (c is '+' || c is '-')
c = *++p;
while (c >= '0' && c <= '9' && p < e)
c = *++p;
}
}
}
return src[0..p - src.ptr];
}
}
debug (UnitTest)
{
    }
debug (Integer)
{
    import tango.io.Stdout;
    void main();
}
