import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";

export default function Home() {
  return (
    <div className="flex min-h-screen items-start justify-center bg-background font-sans pt-5">
      <main className="flex w-full max-w-md flex-col items-center gap-8 px-4">
        <h1 className="text-6xl font-bold tracking-tight">Valerie</h1>
        <Card className="w-full">
          <CardHeader className="gap-0 text-center">
            <CardTitle className="text-xl font-medium">
              Go from Post to Notes
            </CardTitle>
            <CardDescription>
              Stop doomscrolling. Spend more time with your ideas.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="space-y-2">
              <label htmlFor="tiktok-url" className="text-sm font-medium leading-none peer-disabled:cursor-not-allowed peer-disabled:opacity-70">
                Enter a TikTok url:
              </label>
              <Input
                id="tiktok-url"
                type="url"
                placeholder="https://www.tiktok.com/@username/video/..."
              />
            </div>
            <Button className="w-full">Submit</Button>
          </CardContent>
        </Card>
      </main>
    </div>
  );
}
