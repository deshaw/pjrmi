# Usage


## Prerequisites

You will need [Gradle](https://gradle.org/) to build PJRmi. Different Linux
distributions will also need dependent packages to be installed. The specific
versions in the below examples should be adjusted where appropriate.

On Ubuntu you will want some of the following packages:
```bash
sudo apt install build-essential g++ openjdk-11-jdk-headless python3-numpy py3c-dev python3-pip python3-snappy python3-dev libpython3-dev python3-pytest 
```

On RHEL8 you will want some of the following packages:
```bash
sudo yum install gcc-toolset-12-gcc-c++ java-11-openjdk java-11-openjdk-devel python3-numpy python3-pip py3c-devel python3-pytest python3-wheel snappy-devel
pip3 install python-snappy plumbum
```

## Getting started

The easiest way to try out PJRmi is by `pip`-installing it locally, this can be
done with the following command:

```bash
$ ./gradlew :python:develop
$ python -c 'import pjrmi;print(pjrmi.PJRMI_VERSION)' // Smoke test
```

## Build wheel

To build Python wheel, run:

```bash
$ ./gradlew :python:wheel
```
The resultant .whl file can be found in the `.../python` top-level directory.


## Examples

See the `README` for more information.


### Java Child Process

The simplest usage of PJRmi is to create a Java child from the Python process
and interact with it directly:

    >>> import pjrmi
    >>> c = pjrmi.connect_to_child_jvm(stdout=None, stderr=None)
    >>> ArrayList = c.class_for_name('java.util.ArrayList')
    >>> HashMap   = c.class_for_name('java.util.HashMap')
    >>> m = HashMap()
    >>> for i in range(5):
    ...     m.put(i, ArrayList(range(i)))
    >>> str(m)
    {0=[], 1=[0], 2=[0, 1], 3=[0, 1, 2], 4=[0, 1, 2, 3]}'


Flipping the above around, you can create a Python child and drive it from the
Java process. See the `PJRmiTest.java` code for an example of this.


### Java Server

Finally, you can embed a PJRmi server in a Java process and connect it remotely
via an *unsecured* and *unauthenticated* TCP connection. The `PJRmi.java` code
has an example of this in its main method:

    user@host1:~$ java -Djava.library.path=`python -c 'import pjrmi; print(str(pjrmi.get_config()["libpath"]))'` \
                       -classpath `python -c 'import pjrmi; print(str(pjrmi.get_config()["classpath"]))'`        \
                           com.deshaw.pjrmi.PJRmi
    INFO: PJRmi:Socket[65432] Listening for connections with Socket[65432]
    [...]

    user@host2:~$ python
    [...]
    >>> c = pjrmi.connect_to_socket('host1', 65432)
    >>> ArrayList = c.class_for_name('java.util.ArrayList')
    >>> HashMap   = c.class_for_name('java.util.HashMap')
    >>> m = HashMap()
    >>> for i in range(5):
    ...     m.put(i, ArrayList(range(i)))
    >>> str(m)
    {0=[], 1=[0], 2=[0, 1], 3=[0, 1, 2], 4=[0, 1, 2, 3]}'

In order to make this more usable in the Java process one should override the
`getObjectInstance()` method to return objects within the system to which access
is needed.

For a secured connection users can look at the `SSLSocketProvider` class for
guidance. In order to use the SSL socket connection you will need to create a
key store for the server and the client. These files should be created and
handled securely. The client's certificate needs to be imported into the
server's store so that the server can identify that the client is who they say
they are. The CN, as denoted by the `What is your first and last name?` entry,
is what will be returned to the `isUserPermitted()` method.

To populate the stores for the server and client you might do something like the
following, using Java's `keytool` executable. (Java version 9 or higher is
needed for `keytool` to generate PKCS12 stores which Python can parse.)

    user@host:~/keystore$ keytool -genkey -alias serverkey -keyalg RSA -keysize 2048 -sigalg SHA256withRSA -keystore serverkeystore.p12 -storepass password
    What is your first and last name?
      [Unknown]:  the-server
    [...]
    Is CN=the-server, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
      [no]:  yes

    Generating 2,048 bit RSA key pair and self-signed certificate (SHA256withRSA) with a validity of 90 days
            for: CN=the-server, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown

    user@host:~/keystore$ keytool -genkey -alias clientkey -keyalg RSA -keysize 2048 -sigalg SHA256withRSA -keystore clientkeystore.p12 -storepass password
    What is your first and last name?
      [Unknown]:  user
    [...]
    Is CN=user, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown correct?
      [no]:  yes

    Generating 2,048 bit RSA key pair and self-signed certificate (SHA256withRSA) with a validity of 90 days
            for: CN=user, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown

    user@host:~/keystore$ keytool -exportcert -keystore serverkeystore.p12 -alias serverkey -storepass password -rfc -file server-certificate.pem
    Certificate stored in file <server-certificate.pem>
    user@host:~/keystore$ keytool -exportcert -keystore clientkeystore.p12 -alias clientkey -storepass password -rfc -file client-certificate.pem
    Certificate stored in file <client-certificate.pem>

    user@host:~/keystore$ keytool -import -trustcacerts -file server-certificate.pem -keypass password -storepass password -keystore clientkeystore.p12
    Owner: CN=the-server, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown
    Issuer: CN=the-server, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown
    [...]
    Trust this certificate? [no]:  yes
    Certificate was added to keystore

    user@host:~/keystore$ keytool -import -trustcacerts -file client-certificate.pem -keypass password -storepass password -keystore serverkeystore.p12
    Owner: CN=user, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown
    Issuer: CN=user, OU=Unknown, O=Unknown, L=Unknown, ST=Unknown, C=Unknown
    [...]
    Trust this certificate? [no]:  yes
    Certificate was added to keystore
