PJRmi Technical Details
=======================

## Overview

Here we cover some of the technical details of PJRmi. There were a number of
technical challenges to overcome as part of its implementation and some of these
are worthy of note from both an academic as well as a practical standpoint.

Weighing in at almost 40kloc, there's quite a lot to PJRmi under the hood. So
there's plenty to understand for a prospective developer, or a curious
bystander. This document may also be useful as a resource for anyone modifying
or extending PJRmi.


## Type systems

This is arguably the most interesting part of PJRmi, so it's documented here
first. (Yes, I'm a blast at parties.)

When compared with one another, Java and Python's type systems fall into the
"sorta the same but different" category. On the face of it they are pretty
similar; both have a very object-based philosphy, with certain primitive types
being a little special. However, it's their differences which are most pertinent
to PJRmi:
  1. Natively, Python only has `int`, `float`, and `complex` as numeric values;
     Java has
     multiple types for integers and floating point values. Python has bytes
     strings, and read-write bytearays; Java has characters and strings as
     distinct types.
  1. Java has both object type and primitive type versions of basic types
     (integers, floating point values, booleans); Python only has objects.
  1. Java has method overloading by parameter; Python requires all method names
     to be unique. (Python also has keyword arguments to functions.)
  1. Python has runtime-/duck-typing; Java is statically and strictly typed.

We'll examine each of these in turn.


### Numeric types

Like other C-derived languages, Java has multiple primitive types for integers
and floating point values: `byte`, `short`, `int` & `long`, and `float` &
`double`. For Java functions which take a primitive value this doesn't present
much of a problem to PJRmi; you can always check to see if the value you have
been given can be converted to the argument type.

However, if you have a function which takes an `Object` and you are passing in a
Python `int` then the conversion is ambiguous. PJRmi takes a value-based approach
 and will convert the Python value to the smallest representable
type. I.e. if you have `10` you'll get a `Byte`, `1000` a `Short`, `100000` an
`Integer`, and `10000000000` a `Long`.

However, this strategy yields a gotcha when type erasure comes into play.
Container types in Java are generics and their types are only known at compile
time; PJRmi relies on runtime information, accessed via reflection. All you can
tell at runtime is that you have, say, a `List`, not a `List<Integer>`. This
means that, for example, in a `Map<K,V>` the `V get(K k)` method will correctly
compile `get(1)` in Java if `K` is `Integer`. But PJRmi will do the wrong thing
and will _silently_ use a `Byte` as the `k` value. This means that the `get`
call will incorrectly return `null` if the `Map` has been populated with
`Integer`s, because `Byte.valueOf(1).equals(Integer.valueOf(1))` will evaluate
as `false`. Yuck.

The same ambiguity exists for `float`s and `double`s but, given common usage
patterns, PJRmi always uses `double`s to resolve it.

There exist a few workarounds to this:
  1. PJRmi understands the numpy types. That means that it will convert numpy's
     `int8` to a `Byte`, `int16` to a `Short`, and so on. Similarly `float32`
     and `float64` are converted to `Float` and `Double` respectively.
  1. One can call methods like `Integer.valueOf(v)` to explicitly convert the
     given value to the corresponding type.
  1. The [AsType](java/src/main/java/com/deshaw/pjrmi/AsType.java) utility class
     provides a bunch of identity methods which will naturally convert to the
     desired type.
The latter two solutions require a method call. A round-trip call
into the Java side from the Python side will therefore be slower than using
numpy's types to provide the type information.


### Boxing etc.

In Python everything is an `object` but in Java only _almost_ everything is an
`Object`. Java has primitive types for C types, as well as their `Object`
equivalents. In Java, passing an `int` to a function which takes an `Integer`
(or any of its parent classes) will result in the `int` being transparently
"boxed" into an `Integer` object. This is mimicked in PJRmi in two different
forms.

Firstly, PJRmi will transparently attempt to transform a Python object into an
equivalent Java type, by doing a by-value copy. The semantics for this are
uncontroversial for immutable types like integers and floats; their values will
be copied into a corresponding `Object` type on the Java side. The same goes for
strings. (Though see the caveats above when it comes to ambiguities of going
from Python `int`s to Java `Object`s.)

Container types are also passed as by-value copies; a Python `list` will be
copied into a new Java `List`. This means that and changes in the latter are not
reflected in the former. For example, doing calling into Java with
`Collections.sort([5,4,3,2,1])` will have no effect on the argument; the `List`
which `sort()` gets will be created on the fly and immediately discarded. This
is a gotcha.

Secondly, Java Objects which have equivalent types on the Python side will be
"boxed" into subclasses of those Python types. For example, a Java function
which yields a `String` will result in a `str` box on the Python side:

    >>> s = Integer.toString(123)
    >>> s
    '123'
    >>> isinstance(s, str)
    True

However, if you look closely, you will see that it's actually a subclass of
`str`, which has a Java `Object` inside it:

    >>> type(s)
    pjrmi._JavaString
    >>> s.java_object
    <pjrmi.java.lang.String at 0x7f1f813390c0>

These "boxed" types will be naturally unwrapped if you pass them into a Java
method, preserving the `Object` instance:

    >>> System.identityHashCode('123')
    1719137610
    >>> System.identityHashCode('123')
    21712863
    >>> System.identityHashCode(s)
    1010583038
    >>> System.identityHashCode(s)
    1010583038


### Method overloading

This was probably one of the trickiest things to make work in PJRmi. As noted
above the issue is that Python only has one instance of a method with any given
name.

In Java you can have:

```java
public int foo()      { return 0; }
public int foo(int i) { return i; }
```

and calls will bind as one would expect.

However, doing the same in Python:

```python
class Foo:
    def foo(self):
        return 0
    def foo(self, i):
        return i
```

will simply cause the second `foo()` declaration to overwrite the first.

Because there can only be a single method for any given name PJRmi needs to
perform resolution of overloaded Java methods at runtime.

On the face of it the problem is simple, you always want to pick the method
which is most specific to the arguments which are given. If the overloading is
of the form of a different number of arguments then it's not too hard; given the
Java methods `foo()` and `foo(int i)` you know what to do when the Python code
calls `foo(1)`.

Things start to get interesting when you have overloading based on type:

```java
public static String foo(Number  n) { return "Number";  }
public static String foo(Integer n) { return "Integer"; }
```

This is still unambiguous though. This still behaves as one would expect:

    >>> Foo.foo(1)
    'Integer'
    >>> Foo.foo(1.0)
    'Number'

But, when you have more arguments, things are a little trickier:

```java
public static String foo(int n, double d) { return "int-double"; }
public static String foo(double d, int n) { return "double-int"; }
```

On the Python side the unambiguous calls are still fine:

    >>> Foo.foo(1, 1.0)
    'int-double'
    >>> Foo.foo(1.0, 1)
    'double-int'

But for the ambiguous case we get this:

    >>> Foo.foo(1, 1)
    [...]
    TypeError: Call to foo(<class 'int'>, <class 'int'>) is ambiguous; multiple matches: foo(double, int), foo(int, double)

Which matches Java's compilation behaviour:

```
Foo.java:5: error: reference to foo is ambiguous
    public static void main(String... args) { foo(1, 1); }
both method foo(int,double) in Foo and method foo(double,int) in Foo match
```

Like the `Object` type ambiguity mentioned above, this can be handled by adding
richer type information:

    >>> Foo.foo(int32(1), float64(1))
    'int-double'

Or the method can be explicitly unbound and invoked:

    >>> Foo.foo['int','double'](1, 1)
    'int-double'

Under the hood PJRmi performs the resolution by determining "relative
specificities". Each Java method has an associated topological ordering relative
to all its other overloaded forms. That ordering is determined by the
[MethodUtil](java/src/main/java/com/deshaw/pjrmi/MethodUtil.java) class in the
PJRmi Java code. Edge cases around boxing and `null`s  were difficult to make
work correctly.


### Proxy classes and lambdas

One of the really great features of Python is duck-typing; you don't need to
worry about types too much. Provided that the object you give a function
basically looks like what the function expects, things basically work. Java is a
more strict. Imagine that you have two distinct but semantically identical
types, `Foo` and `Bar`, each with just a single method `baz()`. In Java, you
still can't give a `Foo` instance to a function which expects `Bar` one.

PJRmi looks to try to meet the two languages in the middle, provided that `Foo`
and `Bar` are interfaces. One can use Python code to implement a Java interface,
for example, we can use a Python class to duck-type a Java `Runnable`:

    >>> Thread = c.class_for_name('java.lang.Thread')
    >>> class PythonRunnable(pjrmi.JavaProxyBase):
    ...    def run(self):
    ...        print("I ran!")
    >>> thread = Thread(runnable)
    >>> thread.start()
    I ran!

Under the hood this is implemented using the `java.lang.reflect.Proxy` class on
the Java side (which unfortunately only works for interfaces). When the Java side
invokes the object's methods it will issue a callback, back into the Python
side, and return whatever result it got back to the Java side.

Similar to proxy classes, you can use Python lambdas as Java lambdas:

    >>> m = HashMap()
    >>> m.computeIfAbsent(1, lambda x: x+1)
    2
    >>> m.toString()
    '{1=2}'

And, like with the proxy classes above, the Java side will issue a callback into
the Python side in order to evaluate the function.

Callbacks are only enabled in the PJRmi instance if it is using multiple
workers. (See the section on lambdas in the [README](README.md) file, and the
below descussion of the threading model.) This is because the calling thread in
Python will be blocked on waiting for Java, and so it will be unable to respond
to any callback. Instead, calls into both sides are handled by workers in the
callback model. An idle worker will be used for each callback, with new ones
being created on the fly if the pool of workers in exhausted.


## Communication system

At the very heart of PJRmi is the need for two different systems to talk to one
another. From the outset, the intent was for the modes of communication to
have no functional differences; the only difference between talking to a
sub-process vs to a peer on a remote host should be latency.

### Protocol

PJRmi has its own, special purpose, binary protocol. This is documented
by the specific functions which marshall/unmarshall the calls on each side.

Any modications to the protocol are considered to be breaking changes and will
result in the version number of PJRmi being bumped. Instances of PJRmi using
different versions will refuse to handshake and connection attempts will fail.
This isn't a limitation in providing backwards compatability in the protocol;
two different versions will have distinct behaviour, which much be coherent
between them.

### Transports

The communication mechanism is captured by the `Transport` classes, on both the
[Java](src/main/java/com/deshaw/pjrmi/Transport.java) and Python sides. The main
functionailty which these provide is a bi-directional bytestream, which is all
that is really needed for PJRmi to function. Additional methods support features
like security and bulk-data passing.

### Security

The security model of PJRmi comes in two forms, both of which are essentially
ACLs.

The first is access to the server process from a client. In the subprocess model
this is effectively a NOP, since the parent spawns the child and therefore has
full control. However, for a server which is accepting a connection from a
networked client, that client must be authenticated for access to be allowed.
The [SSLTransport](java/src/main/java/com/deshaw/pjrmi/SSLSocketTransport.java)
provides this functionality.

The second form of access control is the allow-list of Java classes which a
Python client may be permitted to access. If a Python client cannot access a
certain class in the Java server then they are unable to invoke methods of that
class. If this access control is enabled in the server then vulnerabilites due
to code injection are mitigated, for obvious reasons.

### Argument passing optimizations

In certain cases the standard communication mechanism can be side-stepped to
give enhanced performance. For a strictly bytestream-based connection all method
arguments must be channeled via the stream; e.g. a `ndarray` of `float64`s will
be marshalled by the Python client, sent over the wire as a bag of bytes, and
unmarshalled as a `double[]` by the Java server.

However, if the client and server are on the same host, then it is possible to
use the host's `/dev/shm` to do fast memory copies of the raw binary data on
both the Python and Java sides. This is handled by the C extension code in both
the Java and Python sides.


## Threading model

PJRmi has two threading models:
  1. Single-threaded: All calls issued from (any thread in) Python are handled
     by the PJRmi thread in the Java server. Callbacks, from Java into Python,
     are not supported.
  1. Worker model: Both Python and Java have worker threads. The Java worker
     threads handle the incoming calls from the Python client. Any callbacks
     from the Java side are handled by Python's worker threads.

In both cases the Java server is handling requests in a thread in the JVM which
is separate from the other threads of the container process. This means that you
need to ensure that any datastructures which the Java server exposes are safe to
access.

If you are in the single-threaded model then you only need to worry about access
from other Java threads. If no such access is being made, e.g. because
everything is driven by the Python client, then access is safe. In the Worker
model, memory barriers will ensure that consistency is maintained between worker
threads.

In the event that multiple thread wish to access Java objects then the
[LockManager](java/src/main/java/com/deshaw/util/concurrent/LockManager.java)
provides high-level locking primitives, including realtime deadlock prevention.

The PJRmi Java server may optionally be instantiated with a global lock. If
present, then the global lock will be exclusively held for the duration of
method calls into the Java side. This global lock is also exposed by the PJRmi
instance so that it can be used to coordinate with other Java threads in the
process.

### Callback support and virtual threads

As noted above, it's possible to recursively call back from Java into Python in
a way which is only bounded by the memory and threading resources of the host
machines.

This is implemented using worker threads in both the Java and Python processes.
However, while there are multiple actual threads in action during a callback,
there is only one _logical_ thread in play. Similar to how one function calling
another, within either Java or Python, only happens in a single thread, the
callbacks only operate within a single logic thread of operation. That this
thread of operation is traversing processes is, really, just an implementation
detail.

This is important because of locking semantics. In an appliction, it's perfectly
safe for the same thread to obtain an exclusive lock multiple times. Similarly,
a logical thread, weaving its way between the Java and Python processes, should
be able to obtain the same exclusive lock multiple times.

In order to support these logical threads there exists the
[VirtualThreadLock](java/src/main/java/com/deshaw/util/concurrent/VirtualThreadLock.java)
class, with its associated `VirtualThread` class. PJRmi internally handles all
of the semantics for you; the user only needs to care about obtaining and using
locks. These are provided by the `LockManager`.

### The Lock Manager

The [LockManager](java/src/main/java/com/deshaw/util/concurrent/LockManager.java)
class handles all the locking semantics for PJRmi. A lock can be obtained on the
Java side by calling the `LockManager`'s various `getBlahLockFor(String name)`
methods. The `LockManager` instance is obtained via the `PJRmi.getLockManager()`
method, provided that the PJRmi instance is constructed with one. Both exclusive
and shared locks are supported on the Java side.

Locks can be obtained on the Python side via the `lock_for_name()` method in the
PJRmi client. (Only exclusive locks are supported on the Python side.)

When a lock is acquired, the `LockManager` will first check to ensure that
deadlock will not happen as a result. If deadlock will occur then a
`DeadlockException` is thrown, and the caller needs to resolve the contention.

Note that, semantically, two Python clients are merely two threads in the Java
process. As such, it's possible for one Python client to deadlock with another
Python client. If that happens, the `DeadlockException` will be thrown in one of
those clients.

Deadlock detection is handled by looking for loops in a dependency graph. This
is explained in detail, complete with ASCII diagrams, in the
`LockManager.lockWalksTo()` method's code. Please refer to that documentation
for more information.

The `LockManager` code is surprisingly performant, given what it's doing.
<can you point to a benchmark>?
The bottom line is that you should use locks if you
need them and should not worry too much about things being slow.


## Other considerations...

A general rule of thumb employed in PJRmi is that we try to ensure that as few
assumptions as possible are made. This includes ones regarding future use; as
such there is little or no specialised logic in the code. Sticking with this
principle has helped to avoid making breaking changes in the code (aside from
changes in the protocol).

The trickiest part of PJRmi is handling the conflicting idioms in Java and
Python in a way which reduces the complexity. Java method capture was a good
example of this; it took a long time before we had a good solution for this.
When we did eventually get one, however, it was nice that it also solved some
other problems as a side effect (e.g. method overloading disambiguation).
