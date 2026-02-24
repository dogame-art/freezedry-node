FROM node:22-slim

WORKDIR /app

# better-sqlite3 needs build tools
RUN apt-get update && apt-get install -y python3 make g++ && rm -rf /var/lib/apt/lists/*

COPY package.json ./
RUN npm install --production

COPY src/ ./src/

# DB directory (mount as volume for persistence)
RUN mkdir -p /app/db

EXPOSE 3100

CMD ["node", "src/server.js"]
