# Bunny Hop delivery constraints

These constraints were explicitly requested and must carry into the implementation plan.

- Implement the game in JavaScript using PixiJS `v8` as the game engine
- Embed and serve it through `deps/rabbitmq_management` at `/bunny-hop`
- Use the supplied RabbitMQ logo from `deps/rabbitmq_prometheus/docker/grafana/publish/rabbitmq-logo.png` as the main character
- Use Google's Dinosaur Game as inspiration for the running and jumping interaction

The feature specification describes observable behaviour. Engine integration, asset packaging, and routing design belong in the plan.

- Use JavaScript only, with no TypeScript source or compiler
- Bind Exit to `q` on all game screens, including loading and failure
- Do not start the management page auto-refresh timer on the game page
- During implementation, use the `pixijs` skill router and the relevant specialised PixiJS skills before editing engine code
