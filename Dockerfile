FROM fedora-minimal:latest

RUN microdnf -y update && microdnf -y install qpid-proton-c && microdnf -y clean all
ADD build/slim-server /

ENTRYPOINT ["/slim-server"]
