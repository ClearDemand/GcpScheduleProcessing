FROM node:18-slim

WORKDIR /app

# Install dependencies first (better layer caching)
COPY package*.json ./
RUN npm install --omit=dev

# App source
COPY . .

# JOB_NAME is supplied per Cloud Run Job at deploy time (see deploy/deploy_jobs.sh)
CMD ["node", "index.js"]
