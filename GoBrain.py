import json
import time
import threading
import websocket
import pygame

# Configuration — Replace with your actual credentials from Emotiv Developer Portal
CLIENT_ID = "YOUR_CLIENT_ID_HERE"
CLIENT_SECRET = "YOUR_CLIENT_SECRET_HERE"
CORTEX_URL = "wss://localhost:6868"

# Shared State variables
app_state = {
    "stress": 0.0,
    "focus": 0.0,
    "push_power": 0.0,
    "is_break_time": False,
    "authenticated": False
}

class EmotivCortexClient:
    def __init__(self):
        self.ws = None
        self.auth_token = ""
        self.session_id = ""

    def start(self):
        # Disable SSL check for local WebSocket server
        self.ws = websocket.WebSocketApp(
            CORTEX_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error
        )
        thread = threading.Thread(target=self.ws.run_forever, kwargs={"sslopt": {"cert_reqs": 0}})
        thread.daemon = True
        thread.start()

    def send_req(self, method, params=None, req_id=1):
        req = {"jsonrpc": "2.0", "method": method, "id": req_id}
        if params:
            req["params"] = params
        self.ws.send(json.dumps(req))

    def on_open(self, ws):
        print("Connected to Cortex WebSocket server.")
        # Step 1: Authorize user
        self.send_req("authorize", {"clientId": CLIENT_ID, "clientSecret": CLIENT_SECRET}, req_id=1)

    def on_message(self, ws, message):
        res = json.loads(message)
        req_id = res.get("id")

        if req_id == 1 and "result" in res:
            self.auth_token = res["result"]["cortexToken"]
            # Step 2: Create session with active headset
            self.send_req("createSession", {"cortexToken": self.auth_token, "status": "open"}, req_id=2)

        elif req_id == 2 and "result" in res:
            self.session_id = res["result"]["id"]
            # Step 3: Subscribe to performance metrics ('met') and mental commands ('com')
            self.send_req("subscribe", {
                "cortexToken": self.auth_token,
                "session": self.session_id,
                "streams": ["met", "com"]
            }, req_id=3)
            app_state["authenticated"] = True
            print("Successfully subscribed to BCI streams!")

        # Process real-time stream data
        if "met" in res:
            # 'met' array layout typically: [interest, stress, relaxation, engagement, excitement, focus]
            met_data = res["met"]
            app_state["stress"] = met_data[1] if len(met_data) > 1 else 0.0
            app_state["focus"] = met_data[5] if len(met_data) > 5 else 0.0

        if "com" in res:
            action, power = res["com"][0], res["com"][1]
            if action == "push":
                app_state["push_power"] = power

    def on_error(self, ws, error):
        print(f"Cortex Connection Error: {error}")

def run_game_loop():
    pygame.init()
    screen = pygame.display.set_mode((600, 400))
    pygame.display.set_caption("Tamagotchi BCI Study Companion")
    clock = pygame.time.Clock()
    font = pygame.font.SysFont("Arial", 22)

    character_x = 50

    running = True
    while running:
        screen.fill((240, 240, 245))

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

        stress = app_state["stress"]
        focus = app_state["focus"]

        # Trigger break mode condition (High Stress > 0.65 or Low Focus < 0.35)
        if stress > 0.65 or (focus < 0.35 and focus > 0.0):
            app_state["is_break_time"] = True

        if not app_state["is_break_time"]:
            # --- STUDY MODE DISPLAY ---
            status_str = f"Study Mode | Stress: {stress:.2f} | Focus: {focus:.2f}"
            text_surface = font.render(status_str, True, (40, 40, 40))
            screen.blit(text_surface, (20, 20))

            # Draw happy Tamagotchi (Green Circle)
            pygame.draw.circle(screen, (76, 175, 80), (300, 200), 50)
            mood_label = font.render("Buddy is studying with you!", True, (76, 175, 80))
            screen.blit(mood_label, (180, 270))

        else:
            # --- MINI-GAME BREAK MODE ---
            status_str = "BREAK TIME! Think 'PUSH' to move your character across!"
            text_surface = font.render(status_str, True, (220, 50, 50))
            screen.blit(text_surface, (20, 20))

            # Move character using live brain 'push' command power
            push_val = app_state["push_power"]
            character_x += int(push_val * 8)

            # Draw character (Blue Circle) and finish line (Red Bar)
            pygame.draw.circle(screen, (33, 150, 243), (character_x, 220), 30)
            pygame.draw.rect(screen, (244, 67, 54), (520, 150, 15, 140))

            # Win condition: reached target line
            if character_x >= 520:
                app_state["is_break_time"] = False
                character_x = 50  # Reset position
                time.sleep(1)    # Brief pause before switching back

        pygame.display.flip()
        clock.tick(30)

    pygame.quit()

if __name__ == "__main__":
    # Start BCI client background thread
    cortex = EmotivCortexClient()
    cortex.start()

    # Start Pygame loop on main thread
    run_game_loop()