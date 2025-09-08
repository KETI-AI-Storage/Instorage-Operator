FROM golang:1.21-alpine 

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

ENV OPERATOR=/usr/local/bin/instorage-operator \
    USER_UID=1001 \
    USER_NAME=operator

# Create non-root user
RUN adduser -D -u ${USER_UID} ${USER_NAME}

# Create required directories
RUN mkdir -p /home/${USER_NAME}/.kube && \
    chown -R ${USER_UID}:${USER_UID} /home/${USER_NAME}

# Copy the operator binary (built by build script)
COPY _output/bin/instorage-operator ${OPERATOR}

# Make binary executable
RUN chmod +x ${OPERATOR}

# Switch to non-root user
USER ${USER_UID}

# Expose health probe port
EXPOSE 8081

ENTRYPOINT ["/usr/local/bin/instorage-operator"]