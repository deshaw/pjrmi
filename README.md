PJRmi
=====

## Overview

PJRmi is an API for performing Remote Method Invocation (RMI, aka RPC) in a Java
process from a Python one. The principle it works by is to create shim objects
in Python which the user will transparently treat as they would most Python
objects, but which actually cause things to happen inside the Java process.

PJRmi does everything by reflection so all you need to implement, as a
service-provider, is the `getObjectInstance()` method for it (see below).

As well as the examples below, you can try out the Jupyter
[notebook](python/tests/pjrmi.ipynb) for live versions.


### Highlights

Some headline features of PJRmi are:
-   Seamless interoperation between Java and Python types.
-   Bidirectional calling; Python to Java, or Java to Python.
-   Support for lambdas and duck-typed Python implementations of Java
    interfaces.
-   Versatile and extensible connectivity options, ranging from in-process
    to network-based.
-   Thread-safe execution, with built-in locking support and asynchronous
    execution via futures.
-   Realtime code injection.

Use-case examples:
-   Scriptification of Java applications.
-   Exposing Java service interfaces to Python clients.
-   Command and control.
-   On-the-fly debugging and/or dev-ops.


### Other work

PJRmi isn't alone in implementing a bridge between Python and Java; some other
implementations are:
-   [JEP](https://pypi.org/project/jep/) embeds CPython in Java through JNI
    allowing Java to call down into Python. It's purely in-process.
-   [py4j](https://www.py4j.org/) allows Python to call into Java. It also
    supports Java calling back into Python so that Python clients can implement
    Java interfaces. It works by communicating over a socket. It is probably
    the most feature-rich of the these implementations.
-   [jpy](https://pypi.org/project/jpy/) is another in-process implementation.
    One of its key features is support for fast pass-by-value operations with
    arrays by use of pointer hand-off.

As well as the feature sets of the above, PJRmi supports complex Java
constructs, has smooth integration of the two languages' type systems, and can
be used in different modes of operation transparently to the user.


## A Simple Example

The framework can be brought up in a number of different ways for
clients to connect to:

-   A PJRmi thread is added to an existing Java process, and Python
    clients may connect over the network
-   A Java JVM is instantiated inside the Python process, with the
    Python process being the only connection
-   A Java child process is launched from within the Python process,
    with the parent process being the only connection
-   A Java JVM spawned within the Python process.

Here is an example of the last of these.

    $ python
    >>> import pjrmi
    >>> c = pjrmi.connect_to_child_jvm()

Now `c` is a `PJRmi` instance and can be used to request information from the
server. There are basically two ways to get information: you can get a reference
to a Java object, or a reference to a Java class.

See also the various `connect_to_blah()` methods in the Python interface.


## By Object

    >>> foo = c.object_for_name('Foo')

You can get a reference to a java object with the command
`c.object_for_name(<string>)`. This calls into a Java method on the server, and
that method will see the string and decide what object to pass back. In this
example it will simply return None (or `null`, from Java's perspective). The
method is left unimplemented in the PJRmi class -- each server will have to
implement it and decide which objects you want to expose to Python. The
stand-alone test server example in `com.deshaw.PJRmi.main` simply does this:

    // Create a simple instance which just echoes back the name it's given
    final PJRmi pjrmi =
        new PJRmi("PJRmi", provider) {
            @Override protected Object getObjectInstance(CharSequence name) {
                return name.toString().intern();
            }
        };

Most of the time Java objects are returned as they are but, in this example,
`foo` will be a special subclass of a Python string; so if we want the Java
String we need to pull it out. Java Objects are generally returned as objects,
only Strings and primitive types (`int`, `double`, `boolean`, ...) have special
boxed handling.

    >>> foo = c.object_for_name('Foo')
    >>> foo
    u'Foo'
    >>> foo = foo.java_string
    >>> foo
    <pjrmi.java.lang.String at 0x39e7c10>
    >>> foo.<tab>
    foo.CASE_INSENSITIVE_ORDER  foo.endsWith                foo.lastIndexOf             foo.startsWith
    foo.charAt                  foo.equals                  foo.length                  foo.subSequence
    foo.codePointAt             foo.equalsIgnoreCase        foo.matches                 foo.substring
    foo.codePointBefore         foo.format                  foo.notify                  foo.toCharArray
    foo.codePointCount          foo.getBytes                foo.notifyAll               foo.toLowerCase
    foo.compareTo               foo.getChars                foo.offsetByCodePoints      foo.toString
    foo.compareToIgnoreCase     foo.getClass                foo.regionMatches           foo.toUpperCase
    foo.concat                  foo.hashCode                foo.replace                 foo.trim
    foo.contains                foo.indexOf                 foo.replaceAll              foo.valueOf
    foo.contentEquals           foo.intern                  foo.replaceFirst            foo.wait
    foo.copyValueOf             foo.isEmpty                 foo.split
    >>> foo.getBytes?
    Type:       function
    String Form:<function getBytes at 0x5fdeb90>
    File:       .../pjrmi.py
    Definition: foo.getBytes(*args, **kwargs)
    Docstring:
    A wrapper for the Java method:
        java.lang.String#getBytes()
    taking the following forms:
        [B getBytes()
        [B getBytes(java.lang.String)
        [B getBytes(java.nio.charset.Charset)
        void getBytes(int, int, [B, int)

When the Python client gets the reply, it gets an id, which is a handle to the
object, and the object's type. If it hasn't seen that type before, it will ask
the Java server to get type information, including the class hierarchy and all
available methods. It will use this to provide ipython tab completion and
documentation.

    >>> foo._<tab>
    foo.__add__           foo.__doc__           foo.__len__           foo.__repr__          foo._bases            foo._is_immutable
    foo.__class__         foo.__eq__            foo.__module__        foo.__setattr__       foo._classname        foo._is_primitive
    foo.__cmp__           foo.__format__        foo.__ne__            foo.__sizeof__        foo._handle           foo._pjrmi
    foo.__del__           foo.__getattribute__  foo.__new__           foo.__str__           foo._hash_code        foo._type_id
    foo.__delattr__       foo.__hash__          foo.__reduce__        foo.__subclasshook__  foo._instance_of
    foo.__dict__          foo.__init__          foo.__reduce_ex__     foo.__weakref__       foo._is_array
    >>> foo._is_immutable
    True
    >>> str(foo)
    'Foo'
    >>> foo._str
    'Foo'


## By Class

Two different ways to get a handle on a Java class as a Python one:

    >>> byte_array = c.class_for_name('[B')
    >>> String     = c.javaclass.java.lang.String

`c.class_for_name(<string>)`, and its syntactic-sugar equivalent, return a
handle to a Java class object, which can be used to call static methods. This
will return any class it knows about without any special handling (unlike
`object_for_name`, for which the server has to have a way of mapping from each
input string to an object).

You can then use the result to create new instances:

    >>> ArrayList = c.javaclass.java.util.ArrayList
    >>> ArrayList([1,2,3])
    <pjrmi.java.util.ArrayList at 0x378a650>
    >>> str(_)
    '[1, 2, 3]'

Note that these classes are fully populated with methods and so forth, as such
tab-completion works on them and reflection-generated docstrings exist also:

    >>> ArrayList.[TAB]
    ArrayList.add             ArrayList.containsAll     ArrayList.hashCode        ArrayList.listIterator    ArrayList.removeAll       ArrayList.toArray
    ArrayList.addAll          ArrayList.ensureCapacity  ArrayList.indexOf         ArrayList.mro             ArrayList.retainAll       ArrayList.toString
    ArrayList.clear           ArrayList.equals          ArrayList.isEmpty         ArrayList.notify          ArrayList.set             ArrayList.trimToSize
    ArrayList.clone           ArrayList.get             ArrayList.iterator        ArrayList.notifyAll       ArrayList.size            ArrayList.wait
    ArrayList.contains        ArrayList.getClass        ArrayList.lastIndexOf     ArrayList.remove          ArrayList.subList

    >>> ArrayList.add?
    Type:       function
    String Form:<function add at 0x37f6de8>
    File:       .../pjrmi/__init__.py
    Definition: ArrayList.add(*args, **kwargs)
    Docstring:
    A wrapper for the Java method:
        java.util.ArrayList#add()
    taking the following forms:
        boolean add(java.lang.Object)
        void add(int, java.lang.Object)


## Automatic Conversion of Python and Java Values

In the above example you'll notice that we pass a Python tuple to the
`ArrayList` constructor. Since one of the constructor's forms is:

    ArrayList(Collection<? extends E> c)

the PJRmi code knows to try to marshall the `tuple` as a `Collection`.
Similarly, since the `ArrayList` is an `Iterable` the PJRmi code also knows that
it can iterate over it in for loops:

    >>> a = ArrayList([1,2,3])
    >>> for i in a:
    ...     print(i)
    ...
    1
    2
    3

Attempts are made to convert other similar types on the fly also.


## Asynchronous Method Calls

It's possible to call a Java method from Python asynchronously, so as to collect
its result at a later point in time. The method will be invoked in a worker
thread and the asynchronous call with return a Java `Future` to eventually reap
the result:

    >>> Thread = c.class_for_name('java.lang.Thread')
    >>> l = tuple(Thread.sleep(10000, sync_mode=c.SYNC_MODE_JAVA_THREAD) for i in range(10))
    >>> c.collect(l)
    # You wait, time passes...
    (None, None, None, None, None, None, None, None, None, None)

The worker threads used by these calls have the following properties:
  - They are different from the call-back ones.
  - They are long-lived.
  - Each one has a unique ID, from the perspective of locking semantics.

Note that the Java server will hold on to the result stored in the `Future`
until `collect()` is called. As such the heap may be exhausted if too many calls
are made before having their results reaped.


## By-value Operations

Since operations on Java objects involve a round-trip to the server it can
sometimes be more efficient to take a copy of a Java value as its equivalent
Python one. This is done using the `PythonPickle` Java code, read by `pickle`
on the Python side.

For example, Java arrays may be converted to Python ones:

    >>> double_a = c.class_for_name('[D')
    >>> array = double_a(100)
    >>> for i in range(len(array)):
    ...     array[i] = i

    >>> type(array)
    pjrmi.[D

    >>> array[10]
    10.0

    >>> python_array = c.value_of(array)
    >>> type(python_array)
    numpy.ndarray

    >>> python_array[10]
    10.0

But remember that this is only a copy of the Java value; changes to the Java
one won't be reflected in the Python one.

    >>> for i in range(len(array)):
    ...    array[i] = 10 * i
    >>> array[10]
    100.0
    >>> python_array[10]
    10.0

You can also get hybrid versions of containers and their elements using the
`best_effort` conversion:

    >>> Object = c.class_for_name('java.lang.Object')
    >>> lst = ArrayList([Object() for _ in range(3)])
    >>> lst.toString()
    '[java.lang.Object@1534bbf7, java.lang.Object@47c234d1, java.lang.Object@657cd6d0]'
    >>> c.value_of(lst, best_effort=True)
    [<pjrmi.java.lang.Object at 0x7f3a7bedb940>,
     <pjrmi.java.lang.Object at 0x7f3a7bedb3a0>,
     <pjrmi.java.lang.Object at 0x7f3a68698130>]

This can be useful, for example, since it means that the Python client doesn't
need to create, and invoke methods on, a Java iterator in order to traverse the
list.


## Type Inference

Sometimes we may have to infer a Java type from a Python one. This can happen
when a method takes a Java `Object` or a generic's type information is lost
owing to type erasure.

For example, imagine that we have a Java method which takes a
`Set<Long>`:

```{#CA-e3c4f9bf65c7011a73b1c9ae80efcee493f774b1 dir="ltr" lang="en"}
import java.util.Set;

public class Foo
{
    public static String toString(Set<Long> set)
    {
        StringBuilder result = new StringBuilder();
        for (Long l : set) {
            if (result.length() > 0) {
                result.append(',');
            }
            result.append(l);
        }
        return result.toString();
    }
}
```

One might imagine that doing this would work, but it does not:

    >>> s = set(range(10))
    >>> Foo.toString(s)
    [...]
        java.lang.ClassCastException: java.lang.ClassCastException: java.lang.Byte cannot be cast to java.lang.Long
            at Foo.toString(Foo.java:8)

The reason is that Python only knows that it's got a set of integers, it doesn't
know anything about them so it simply makes the best guess it can; here, since
the values all fit into a byte, it's using bytes to represent them.

We can fix this by supplying type information from within Python:

    >>> s = set(numpy.arange(10, dtype='int64'))
    >>> Foo.toString(s)
    u'0,1,2,3,4,5,6,7,8,9'


## Casting

    >>> ArrayList = c.class_for_name('java.util.ArrayList')
    >>> a = ArrayList([1,2,3])

There is some amount of automatic casting between Python and Java. For example,
here PJRmi sees that the Python list is automatically converted into a Java
Collection to create an `ArrayList`.

    >>> a0 = a[0]
    >>> a0
    1
    >>> a0.java_object
    <pjrmi.java.lang.Byte at 0x378a5d0>

At run time, Generic objects like `ArrayLists` carry no type information about
the objects they contain. As such, objects returned by methods with the generic
type are cast to their actual value. Here it happens to be a `Byte` which is
wrapped up as a Python int but it might also be something like this:

    >>> a = ArrayList([ArrayList(), ArrayList(), ArrayList()])
    >>> a[0]
    <pjrmi.java.util.ArrayList at 0x378a6d0>

If, however, you need to perform an actual cast from one type into another you
can use the `cast_to()` method:

    >>> ArrayList = c.javaclass.java.util.ArrayList
    >>> List = c.javaclass.java.util.List
    >>> a = ArrayList([1,2,3])
    >>> c.cast_to(a, List)
    <pjrmi.java.util.List at 0x378ac90>


## Extending support to other types

Sometimes users might want to have PJRmi understand how to convert from
user-defined types in Python and Java.

Let's imagine and `int`s and `String`s are special types for an (edited)
example:

    >>> Integer = c.class_for_name('java.lang.Integer')
    >>> Integer.parseInt(1)
    TypeError: Could not find a method matching java.lang.Integer#parseInt(<class 'int'>): Don't know how to turn '1' <class 'int'> into a <java.lang.String>

PJRmi can be extended to understand how to convert from a particular Python type
to a particular Java one by overriding the `PJRmi._format_by_class` method. We
create a lamdba which can be invoked on the Java side to turn the given value
into the desired Java object. Note that no type checking is done to ensure that
the object returned by the Java method is what is desired.

    >>> class MyPJRmi(pjrmi.PJRmi):
    ...     def connect(self):
    ...         super().connect()
    ...         # Remember these classes so we can use them later. We capture
    ...         # these after we have connected the the Java process.
    ...         self._my_java_lang_String = self.class_for_name('java.lang.String')
    ...         self._my_java_lang_Object = self.class_for_name('java.lang.Object')
    ...
    ...     def _format_by_class(self, klass, value,
    ...                          strict_types=True, allow_format_shmdata=True):
    ...         try:
    ...             return super()._format_by_class(klass, value,
    ...                                             strict_types=strict_types,
    ...                                             allow_format_shmdata=allow_format_shmdata)
    ...         except Exception:
    ...             # See if we are trying to marshall the value as a String
    ...             if klass._type_id == self._my_java_lang_String._type_id:
    ...                 # Turn the value into a String on the Java side by invoking
    ...                 # the String.valueOf(Object) method on it
    ...                 method = self._my_java_lang_String.valueOf[self._my_java_lang_Object]
    ...                 return super()._format_as_lambda(method, value,
    ...                                                  strict_types=strict_types,
    ...                                                  allow_format_shmdata=allow_format_shmdata)
    ...             else:
    ...                 raise
    >>> c = pjrmi.connect_to_child_jvm(stdout=None, stderr=None, impl=MyPJRmi)
    >>> Integer = c.class_for_name('java.lang.Integer')
    >>> Integer.parseInt(1)
    1


## Lambdas and Duck-typed Classes

PJRmi also supports the use of Python functions as Java lambdas, as well as
Python classes implementing Java interfaces. In order to do this the Java side
has to be able to call into the Python side (the reverse or what normally
happens); this means that we need to be using a multi-threaded, worker-based
pair. You can start a Java child with two workers from within Python like this:

    >>> c = pjrmi.connect_to_child_jvm(
    >>>                stdout=None,
    >>>                stderr=None,
    >>>                application_args=('num_workers=2',)
    >>>            )

Now, we'll create a `Map` and call the computeIfAbsent() method on it,
using a Python lambda as the provider function:

    >>> HashMap = c.class_for_name('java.util.HashMap')
    >>> m = HashMap()
    >>> m.computeIfAbsent(1, lambda x: x + 1)
    2

Similarly, we can implement a Java interface using a Python class and pass that
into a function. Provided that all the required methods are present in a Python
class you can use that as an implementation of an interface. The `JavaProxyBase`
class in PJRmi provides the standard required methods (like `equals()` and
`hashCode()`) leaving the actual interface methods for you to create yourself.
Here we'll implement a Java `Runnable` and give that to a Java `Thread` to
invoke:

    >>> Thread = c.class_for_name('java.lang.Thread')
    >>> class PythonRunnable(pjrmi.JavaProxyBase):
    ...    def run(self):
    ...        print("I ran!")
    >>> runnable = PythonRunnable()
    >>> runnable.run()
    I ran!
    >>> thread = Thread(runnable)
    >>> thread.start()
    I ran!

The `protected int PJRmi.numWorkers()` method can be overridden in Java servers
to provide callback support for PJRmi server processes.


## Native/CPython Array Handling

It is possible for PJRmi to handle pass-by-reference arrays using `C++`
mechanisms that are relatively fast, specifically `memcpy()` and `mmap()`. This
leads to a significant increase in the speed of data transfer both from Java to
Python and Python to Java. Currently, this functionality only works for
sub-processes since it tunnels the data via `/dev/shm`. From the Python side, it
is enabled setting the `kwarg_use_shm_arg_passing` to `True` when establishing
the connection:

    >>> _pjrmi_connection = pjrmi.connect_to_child_jvm(use_shm_arg_passing=True, ...)

In Java, set the `useShmArgPassing` flag to `true` when spawning the
`PythonMinion`.

``` {#CA-773c86c39fe6160476980e67a95bc1adf045cebd dir="ltr" lang="en"}
   1 private static final PythonMinion PYTHON = PythonMinionProvider.spawn(true)
   2 // or
   3 private static final PythonMinion PYTHON = PythonMinionProvider.spawn(myStdinFilename,
   4                                                                       myStdoutFilename,
   5                                                                       myStderrFilename,
   6                                                                       true)
```


## Dynamic Java Source Injection

PJRmi supports dynamic compilation of Java source code from a `str` in Python.
This can eliminate the need to conduct particularly "chatty" communication
through PJRmi; for example, when iterating through a Java
`ArrayList<HashMap<Integer,Integer>>` object from Python. The compilation uses
the `JavaCompiler` and can be used as follows:

    >>> class_name = "TestInjectSource"
    >>> source     = """
        public class TestInjectSource {
            public static int foo(int i) {
                return i+1;
           }
        }
        """
    >>> Foo = c.inject_source(class_name, source)
    >>> foo = Foo()
    >>> foo.foo(1)
    2


## Java Method Capture

PJRmi supports Java method capture, allowing them to be passed in as
`FunctionalInterface` (i.e. lambda) arguments. They can also be used standalone,
like a captured method in Python.

The sugar syntax is as follows. A `slice` or `Ellipsis` is a wildcard match, but
these will only work when there is no overloading (i.e. no disambiguation is
required).

| Syntax                        | Description                              |
| ----------------------------- | ---------------------------------------- |
| `Class.method[None]`          | Method with no arguments                 |
| `Class.method[:]`             | Method with some, but any, arguments     |
| `Class.method[...]`           | Method with some, but any, arguments     |
| `Class.method[t0, t1, . . .]` | Method with explicitly defined arguments |

For example, the Java Map has a method which takes a lambda:

    public V computeIfAbsent(K key, Function<? super K,? extends V> mappingFunction)

And can be used accordingly:

    >>> HashMap    = c.class_for_name('java.util.HashMap')
    >>> Integer    = c.class_for_name('java.lang.Integer')
    >>> jint       = c.class_for_name('int')

    >>> m = HashMap()
    >>> m.computeIfAbsent(12345678, Integer.toString[jint])
    '12345678'
    >>> m
    {12345678=12345678}

As with Python, a method captured from an instance will be associated with that
instance. When used as lambdas, methods captured from a class will either need
to be static, or invoked by passing in a `this` pointer.

    >>> m = HashMap()
    >>> m.put(1,2)
    >>> m
    {1=2}
    >>> p = m.put[:]
    >>> p(2,3)
    >>> m
    {1=2, 2=3}
    >>> m.computeIfAbsent('hello', String.hashCode[None])
    99162322
    >>> m
    {1=2, 2=3, hello=99162322}

With a Java captured method you can potentially avoid call overhead incurred by
repeatedly calling into Java from the Python side. For example, when we employ a
mapping operation on the Java side instead, we only need to make one call to do
so, instead of many.

    # Get a list of Integers, accounting for the fact that they are a boxed type
    # on the Python side
    >>> l = list(Integer.valueOf(i).java_object for i in range(100000))

    # Apply the instance method toString() on each of them, from the Python side
    >>> %time _ = list(map(Integer.toString[None], l))
    CPU times: user 2.74 s, sys: 383 ms, total: 3.13 s
    Wall time: 3.03 s

    # Apply the same method, but all on the Java side. We assume that we have a
    # function like this in a special utility class:
    #     public static <T,U> List<U> map(final Collection<T> c,
    #                                     final Function<T,U> f)
    # which we will use.
    >>> %time _ = CollectionUtilities.map(l, Integer.toString[None])
    CPU times: user 130 ms, sys: 2.94 ms, total: 133 ms
    Wall time: 153 ms

Methods and constructors can be captured explicitly via the `get_bound_method()`
method, or via syntactic sugar using `[]`s. Passing in the Java types of the
arguments can be used to disambiguate overloaded methods. Both the explicit
capture and the sugar accept either class instances or Java's type names as
arguments.

    >>> str(c.get_bound_method(Integer.toString, arg_types=(jint,)))
    'java.lang.Integer::toString'
    >>> str(c.get_bound_method(Integer.toString, arg_types=('int',)))
    'java.lang.Integer::toString'
    >>> str(Integer.toString[jint])
    'java.lang.Integer::toString'
    >>> str(Integer.toString['int'])
    'java.lang.Integer::toString'

Captured methods can also be used to handle overloading ambiguities:

    >>> l = list(range(10))
    >>> Arrays.binarySearch(l, 5)
    ---------------------------------------------------------------------------
    TypeError                                 Traceback (most recent call last)
    Input In [14], in <cell line: 1>()
    ----> 1 Arrays.binarySearch(l, 5)
    [...]
    TypeError: Call to binarySearch(<class 'list'>, <class 'int'>) is ambiguous; multiple matches: binarySearch([B, byte), binarySearch([J, long), binarySearch([I, int), binarySearch([S, short), binarySearch([Ljava.lang.Object;, java.lang.Object), binarySearch([D, double), binarySearch([F, float)
    >>> Arrays.binarySearch['[I', 'int'](l, 5)
    5


## Technical notes

### Threading Model

The PJRmi service runs as a separate thread inside the Java process. As such you
have to figure out any thread-safety issues. The framework provides support for
a `ReentrantLock` which will be held for the duration of calls which can help in
this.


### Requirements

PJRmi uses features in Java11 and later, and Python 3.6 and later.


## History

PJRmi was contributed back to the community by the [D. E. Shaw group](https://www.deshaw.com/).

<p align="center">
    <a href="https://www.deshaw.com">
       <img src="https://www.deshaw.com/assets/logos/blue_logo_417x125.png" alt="D. E. Shaw Logo" height="75" >
    </a>
</p>


## License

This project is released under a [BSD-3-Clause license](https://github.com/deshaw/pjrmi/blob/master/LICENSE.txt).
