# Lightweight Node.js image for HTTP-only scraping
FROM apify/actor-node:20

# Copy package files
COPY package*.json ./

# Install production dependencies
RUN npm --quiet set progress=false \
    && npm install --omit=dev --omit=optional \
    && echo "Installed NPM packages:" \
    && (npm list --omit=dev --all || true) \
    && echo "Node.js version:" \
    && node --version \
    && echo "NPM version:" \
    && npm --version \
    && rm -r ~/.npm

# Copy source code
COPY . ./

# Run the actor
CMD ["node", "src/main.js"]
