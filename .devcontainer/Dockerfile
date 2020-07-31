FROM centos:8


RUN yum -y install git cmake make libxslt docbook-style-xsl \
    gcc gcc-c++ gdb yum-utils rpm-build rpmlint epel-release && \
    yum -y update

RUN yum-config-manager --add-repo \
    http://research.cs.wisc.edu/htcondor/yum/repo.d/htcondor-stable-rhel8.repo && \
    rpm --import https://research.cs.wisc.edu/htcondor/yum/RPM-GPG-KEY-HTCondor && \
    yum -y install condor-classads condor-classads-devel

RUN yum -y install globus-common globus-io globus-gssapi-gsi globus-gss-assist \
    globus-gsi-credential globus-gsi-proxy-core globus-common globus-io-devel \
    globus-gssapi-gsi-devel globus-gss-assist-devel \
    globus-gsi-credential-devel globus-gsi-proxy-core-devel