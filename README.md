# Transit Reliability Score

A mobile application providing reliability metrics for Metro Vancouver (TransLink) transit services.

## Data Attribution

Transit data provided by TransLink. This data is provided "as is" without warranty.

For TransLink Open API Terms of Use, see: https://www.translink.ca/about-us/doing-business-with-translink/app-developer-resources/translink-open-api

## Stack

- **Mobile**: React Native + Expo + TypeScript
- **Map**: @rnmapbox/maps
- **Backend**: FastAPI (Python 3.12)
- **Database**: PostgreSQL
- **Auth**: Supabase Auth (email magic link)
- **Cache**: Redis (optional)

## Project Structure

```
/
├── apps/
│   ├── mobile/          # React Native Expo app
│   └── api/             # FastAPI backend
├── packages/
│   ├── shared-types/    # Shared TypeScript types
│   └── config/          # Shared configuration
└── infrastructure/
    └── docker/          # Docker configurations
```

## Prerequisites

- Node.js >= 20.0.0
- pnpm >= 9.0.0
- Python >= 3.12
- PostgreSQL >= 16
- Docker (optional, for local development)

## Quick Start

### 1. Install dependencies

```bash
pnpm install
```

### 2. Set up environment variables

```bash
# API
cp apps/api/.env.example apps/api/.env
# Edit apps/api/.env with your values

# Mobile
cp apps/mobile/.env.example apps/mobile/.env
# Edit apps/mobile/.env with your values
# Expo loads these values via `apps/mobile/app.config.js` into `Constants.expoConfig.extra`
```

### 3. Start PostgreSQL (using Docker)

```bash
cd infrastructure/docker
docker-compose up -d postgres
```

### 4. Start the API

```bash
pnpm dev:api
```

### 5. Start the mobile app

```bash
pnpm dev:mobile
```

## Development

### Linting

```bash
# Lint all packages
pnpm lint

# Fix lint issues
pnpm lint:fix
```

### Type checking

```bash
pnpm typecheck
```

### Testing

```bash
pnpm test
```

## API Endpoints

- `GET /health` - Health check endpoint
- `GET /meta/attribution` - Data attribution information

(More endpoints coming in later stages)

## License

Private - All rights reserved.
